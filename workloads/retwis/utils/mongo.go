package utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const kLocalhostMongoUri = "mongodb://localhost:27017"
const kLocalhostMysqlUri = "mysql://localhost:3306" //Set the local mysql instance url which is just localhost port 3306

func getMongoUri() string {
	if uri, exists := os.LookupEnv("MONGODB_URI"); exists {
		return uri
	} else {
		return kLocalhostMongoUri
	}
}

//Return the Static URI for Mysql Database instance set by the Docker-container
func getMysqlUri() string {
	if uri, exists := os.LookupEnv("MYSQL_URI"); exists {
		return uri
	} else {
		return kLocalhostMysqlUri
	}
}

func CreateMongoClientOrDie(ctx context.Context) *mongo.Client {
	uri := getMongoUri()
	opts := options.Client()
	opts.ApplyURI(uri)
	opts.SetReadConcern(readconcern.Majority())
	opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	opts.SetReadPreference(readpref.PrimaryPreferred())
	newCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if client, err := mongo.Connect(newCtx, opts); err != nil {
		log.Fatalf("[FATAL] Failed to connect to mongo %s: %v", uri, err)
		return nil
	} else {
		return client
	}
}

func CreateMysqlClientOrDie(ctx context.Context) (*sql.DB) {
	uri := getMysqlUri()

	//Apply Contexts
	newCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	//You can't actually apply a context to a db-connection with mysql but you can on the query so we'll just do that
	db, err := sql.Open("mysql", uri)
	if err != nil {
		log.Fatalf("[FATAL] Failed to connect to mysql instance at %s: %v", uri, err)
		return nil
	}

	if err := db.PingContext(newCtx); err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}

	
	return db
	
}

func MongoTxnOptions() *options.TransactionOptions {
	opts := options.Transaction()
	opts.SetReadConcern(readconcern.Snapshot())
	opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	opts.SetReadPreference(readpref.Primary())
	return opts
}

func MongoCreateCounter(ctx context.Context, db *mongo.Database, name string) error {
	collection := db.Collection("counters")
	_, err := collection.InsertOne(ctx, bson.D{{"name", name}, {"value", int32(0)}})
	return err
}

func MongoFetchAddCounter(ctx context.Context, db *mongo.Database, name string, delta int) (int, error) {
	collection := db.Collection("counters")

	//TODO: Figure out what this does? What values are being returned
	filter := bson.D{{"name", name}}
	update := bson.D{{"$inc", bson.D{{"value", int32(delta)}}}}
	var updatedDocument bson.M
	err := collection.FindOneAndUpdate(ctx, filter, update).Decode(&updatedDocument)
	if err != nil {
		return 0, err
	}
	if value, ok := updatedDocument["value"].(int32); ok {
		return int(value), nil
	} else {
		return 0, fmt.Errorf("%s value cannot be converted to int32", name)
	}

	// Determine what the latest value is so that you can create a new UserID		
	
}
 //TODO: Determine parameters to not re-establish connections
 //TODO: delta?
func MysqlFetchAddCounter(ctx context.Context, db *sql.DB, delta int) (int, error) {

	//Workaround - Count the number of rows and the answer will be the userID of the next-user cause mysql starts @0
	//Makes Query within Context
	results,err := db.QueryContext(ctx, "SELECT COUNT(*) FROM users")
	defer results.Close()
	// fmt.Println(results)

	var (
		id int
	)

	for results.Next() {
		results.Scan(&id)
		fmt.Println(id)
	}
	
	if err != nil {
		return 0, fmt.Errorf("%s value cannot be converted to int32")
	}
	//Return error if necesarry

	return id, nil

}


func MongoCreateIndex(ctx context.Context, collection *mongo.Collection, key string, unique bool) error {
	indexOpts := options.Index().SetUnique(unique)
	mod := mongo.IndexModel{
		Keys:    bson.M{key: 1},
		Options: indexOpts,
	}
	_, err := collection.Indexes().CreateOne(ctx, mod)
	return err
}
