package mongodb

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	_ client.Reader = &Reader{}

	// DefaultCollectionFilter is an empty map of empty maps
	DefaultCollectionFilter = map[string]CollectionFilter{}
)

// CollectionFilter is just a typed map of strings of map[string]interface{}
type CollectionFilter map[string]interface{}

type MDocElem bson.DocElem

//func (m MDocElem) String() string {
//	return fmt.Sprintf("%+v", m.Value)
//}

// Reader implements the behavior defined by client.Reader for interfacing with MongoDB.
type Reader struct {
	tail              bool
	collectionFilters map[string]CollectionFilter
	oplogTimeout      time.Duration
}

func newReader(tail bool, filters map[string]CollectionFilter) client.Reader {
	return &Reader{tail, filters, 5 * time.Second}
}

type resultDoc struct {
	doc bson.M
	c   string
}

type iterationComplete struct {
	oplogTime bson.MongoTimestamp
	c         string
}

func (r *Reader) Read(resumeMap map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session).mgoSession.Copy()
		go func() {
			defer func() {
				session.Close()
				close(out)
			}()
			log.With("db", session.DB("").Name).Infoln("starting Read func")
			collections, err := r.listCollections(session.Copy(), filterFn)
			if err != nil {
				log.With("db", session.DB("").Name).Errorf("unable to list collections, %s", err)
				return
			}
			var wg sync.WaitGroup
			for _, c := range collections {
				var lastID interface{}
				oplogTime := timeAsMongoTimestamp(time.Now())
				var mode commitlog.Mode // default to Copy
				if m, ok := resumeMap[c]; ok {
					lastID = m.Msg.Data().Get("_id")
					mode = m.Mode
					oplogTime = timeAsMongoTimestamp(time.Unix(m.Timestamp, 0))
				}
				if mode == commitlog.Copy {
					if err := r.iterateCollection(r.iterate(lastID, session.Copy(), c), out, done, int64(oplogTime)>>32); err != nil {
						log.With("db", session.DB("").Name).Errorln(err)
						return
					}
					log.With("db", session.DB("").Name).With("collection", c).Infoln("iterating complete")
				}
				if r.tail {
					wg.Add(1)
					log.With("collection", c).Infof("oplog start timestamp: %d", oplogTime)
					go func(wg *sync.WaitGroup, c string, o bson.MongoTimestamp) {
						defer wg.Done()
						errc := r.tailCollection(c, session.Copy(), o, out, done)
						for err := range errc {
							log.With("db", session.DB("").Name).With("collection", c).Errorln(err)
							return
						}
					}(&wg, c, oplogTime)
				}
			}
			log.With("db", session.DB("").Name).Infoln("Read completed")
			// this will block if we're tailing
			wg.Wait()
			return
		}()

		return out, nil
	}
}

func (r *Reader) listCollections(mgoSession *mgo.Session, filterFn func(name string) bool) ([]string, error) {
	defer mgoSession.Close()
	var colls []string
	db := mgoSession.DB("")
	collections, err := db.CollectionNames()
	if err != nil {
		return colls, err
	}
	log.With("db", db.Name).With("num_collections", len(collections)).Infoln("collection count")
	for _, c := range collections {
		if filterFn(c) && !strings.HasPrefix(c, "system.") {
			log.With("db", db.Name).With("collection", c).Infoln("adding for iteration...")
			colls = append(colls, c)
		} else {
			log.With("db", db.Name).With("collection", c).Infoln("skipping iteration...")
		}
	}
	log.With("db", db.Name).Infoln("done iterating collections")
	return colls, nil
}

func (r *Reader) iterateCollection(in <-chan message.Msg, out chan<- client.MessageSet, done chan struct{}, origOplogTime int64) error {
	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			out <- client.MessageSet{
				Msg:       msg,
				Timestamp: origOplogTime,
			}
		case <-done:
			return errors.New("iteration cancelled")
		}
	}
}

func (r *Reader) iterate(lastID interface{}, s *mgo.Session, c string) <-chan message.Msg {
	msgChan := make(chan message.Msg)
	go func() {
		defer func() {
			s.Close()
			close(msgChan)
		}()
		db := s.DB("").Name
		canReissueQuery := r.requeryable(c, s)
		for {
			log.With("collection", c).Infoln("iterating...")
			session := s.Copy()
			iter := r.catQuery(c, lastID, session).Iter()
			var result bson.M
			for iter.Next(&result) {
				if id, ok := result["_id"]; ok {
					lastID = id
				}
				msgChan <- message.From(ops.Insert, c, data.Data(result))
				result = bson.M{}
			}
			if err := iter.Err(); err != nil {
				log.With("database", db).With("collection", c).Errorf("error reading, %s", err)
				session.Close()
				if canReissueQuery {
					log.With("database", db).With("collection", c).Errorln("attempting to reissue query")
					time.Sleep(5 * time.Second)
					continue
				}
				return
			}
			iter.Close()
			session.Close()
			return
		}
	}()
	return msgChan
}

func (r *Reader) catQuery(c string, lastID interface{}, mgoSession *mgo.Session) *mgo.Query {
	query := bson.M{}

	//if f, ok := r.collectionFilters[c]; ok {
	//	query = bson.M(f)
	//}
	//  map[createdAt:map[$gte:2019-04-02 20:54:11.353 +0000 UTC]]
	//  map[createdAt:map[$gte:2017-04-02 20:54:11.353 +0000 UTC]]
	// filter: { createdAt: { $gte: new Date(1554238451353) } }
	// filter: { createdAt: { name: "createdAt", value: { $gte: { name: "$gte", value: new Date(1491166451353) } } } }
	query, err := parseMap(r.collectionFilters[c], false)
	if err != nil {
		panic(err)
	}

	// t, err := time.Parse(time.RFC3339, "2019-04-02T20:54:11.353Z")
	// if err != nil {
	// 	panic(err)
	// }
	//log.Infoln("queryString created: ", bson.M{"createdAt": bson.M{"$gte": t}})
	//query = bson.M{"createdAt": bson.M{"$gte": t}}
	//query = bson.M{"$or": []bson.M{bson.M{"createdAt": bson.M{"$gte": t}}, bson.M{"createdAt": bson.M{"$gte": t}}}}
	//log.Infoln("queryString created: ", bson.M{"$or": []bson.M{bson.M{"createdAt": bson.M{"$gte": t}}, bson.M{"createdAt": bson.M{"$gte": t}}}})
	if lastID != nil {
		query["_id"] = bson.M{"$gt": lastID}
	}

	log.Infoln("queryString : ", query)
	return mgoSession.DB("").C(c).Find(query).Sort("_id")
}

func (r *Reader) requeryable(c string, mgoSession *mgo.Session) bool {
	db := mgoSession.DB("")
	indexes, err := db.C(c).Indexes()
	if err != nil {
		log.With("database", db.Name).With("collection", c).Errorf("unable to list indexes, %s", err)
		return false
	}
	for _, index := range indexes {
		if index.Key[0] == "_id" {
			var result bson.M
			err := db.C(c).Find(nil).Select(bson.M{"_id": 1}).One(&result)
			if err != nil {
				log.With("database", db.Name).With("collection", c).Errorf("unable to sample document, %s", err)
				break
			}
			if id, ok := result["_id"]; ok && sortable(id) {
				return true
			}
			break
		}
	}
	log.With("database", db.Name).With("collection", c).Infoln("invalid _id, any issues copying will be aborted")
	return false
}

func sortable(id interface{}) bool {
	switch id.(type) {
	case bson.ObjectId, string, float64, int64, time.Time:
		return true
	}
	return false
}

func (r *Reader) tailCollection(c string, mgoSession *mgo.Session, oplogTime bson.MongoTimestamp, out chan<- client.MessageSet, done chan struct{}) chan error {
	errc := make(chan error)
	go func() {
		defer func() {
			mgoSession.Close()
			close(errc)
		}()

		var (
			collection = mgoSession.DB("local").C("oplog.rs")
			result     oplogDoc // hold the document
			db         = mgoSession.DB("").Name
			ns         = fmt.Sprintf("%s.%s", db, c)
			query      = bson.M{"ns": ns, "ts": bson.M{"$gte": oplogTime}}
			iter       = collection.Find(query).LogReplay().Sort("$natural").Tail(r.oplogTimeout)
		)
		defer iter.Close()

		for {
			log.With("db", db).Infof("tailing oplog with query %+v", query)
			select {
			case <-done:
				log.With("db", db).Infoln("tailing stopping...")
				return
			default:
				for iter.Next(&result) {
					if result.validOp(ns) {
						var (
							doc bson.M
							err error
							op  ops.Op
						)
						switch result.Op {
						case "i":
							op = ops.Insert
							doc = result.O
						case "d":
							op = ops.Delete
							doc = result.O
						case "u":
							op = ops.Update
							doc, err = r.getOriginalDoc(result.O2, c, mgoSession)
							if err != nil {
								// errors aren't fatal here, but we need to send it down the pipe
								log.With("ns", result.Ns).Errorf("unable to getOriginalDoc, %s", err)
								continue
							}
						}

						msg := message.From(op, c, data.Data(doc)).(*message.Base)
						msg.TS = int64(result.Ts) >> 32

						out <- client.MessageSet{
							Msg:       msg,
							Timestamp: msg.TS,
							Mode:      commitlog.Sync,
						}
						oplogTime = result.Ts
					}
					result = oplogDoc{}
				}
			}

			if iter.Timeout() {
				continue
			}
			if iter.Err() != nil {
				log.With("path", db).Errorf("error tailing oplog, %s", iter.Err())
				mgoSession.Refresh()
			}

			query = bson.M{"ts": bson.M{"$gte": oplogTime}}
			iter = collection.Find(query).LogReplay().Tail(r.oplogTimeout)
			time.Sleep(100 * time.Millisecond)
		}

	}()
	return errc
}

// getOriginalDoc retrieves the original document from the database.
// transporter has no knowledge of update operations, all updates work as wholesale document replaces
func (r *Reader) getOriginalDoc(doc bson.M, c string, s *mgo.Session) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("can't get _id from document")
	}

	query := bson.M{}
	if f, ok := r.collectionFilters[c]; ok {
		query = bson.M(f)
	}
	query["_id"] = id

	err = s.DB("").C(c).Find(query).One(&result)
	if err != nil {
		err = fmt.Errorf("%s.%s %v %v", s.DB("").Name, c, id, err)
	}
	return
}

// oplogDoc are representations of the mongodb oplog document
// detailed here, among other places.  http://www.kchodorow.com/blog/2010/10/12/replication-internals/
type oplogDoc struct {
	Ts bson.MongoTimestamp `bson:"ts"`
	H  int64               `bson:"h"`
	V  int                 `bson:"v"`
	Op string              `bson:"op"`
	Ns string              `bson:"ns"`
	O  bson.M              `bson:"o"`
	O2 bson.M              `bson:"o2"`
}

// validOp checks to see if we're an insert, delete, or update, otherwise the
// document is skilled.
func (o *oplogDoc) validOp(ns string) bool {
	return ns == o.Ns && (o.Op == "i" || o.Op == "d" || o.Op == "u")
}

func timeAsMongoTimestamp(t time.Time) bson.MongoTimestamp {
	return bson.MongoTimestamp(t.Unix() << 32)
}

func parseTime(val string) (time.Time, error) {
	t, err := time.Parse("2006-01-02T15:04:05.999Z", val)
	return t, err
}

func parseInterface(key string, val interface{}, timeFlag bool) (MDocElem, interface{}, error) {
	result := bson.DocElem{Name: key}
	//xType := reflect.TypeOf(val)
	//xValue := reflect.ValueOf(val)
	//fmt.Println(xType, xValue)
	switch val.(type) {
	case map[string]interface{}:

		if (key == "createdAt") || (key == "updatedAt") || (key == "deletedAt") {
			timeFlag = true
		} else {
			timeFlag = false
		}
		value, err := parseMap(val.(map[string]interface{}), timeFlag)
		if err != nil {
			//error while parsing the map
			return MDocElem(result), "", err
		}
		result.Value = value
	case []interface{}:
		value, err := parseArray(val.([]interface{}))
		if err != nil {
			//error while parsing the array
			return MDocElem(result), "", err
		}
		result.Value = value
	case string:
		if !timeFlag {
			break
		}
		value, err := parseTime(val.(string))
		if err != nil {
			//error while parsing the time
			return MDocElem(result), "", err
		}
		result.Value = value
	default:
		result.Value = val
	}
	return MDocElem(result), result.Value, nil
}

func parseMap(aMap map[string]interface{}, timeFlag bool) (bson.M, error) {

	result := bson.M{}

	for key, val := range aMap {
		//fmt.Println("key 0", key)
		//fmt.Println("Value 0 ", val)
		value, realValue, err := parseInterface(key, val, timeFlag)
		if err != nil {
			return nil, err
		}
		//fmt.Println("key 1", key)
		log.Infoln("Value from parseMap : ", value, realValue)
		result[key] = realValue
	}
	return result, nil
}

func parseArray(anArray []interface{}) ([]interface{}, error) {
	result := []interface{}{}
	for _, val := range anArray {
		value, realValue, err := parseInterface("", val, false)
		if err != nil {
			return nil, err
		}
		log.Infoln("Value from parseArray : ", value, realValue)
		result = append(result, realValue)
	}
	return result, nil
}
