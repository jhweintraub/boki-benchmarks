package handlers

import (
	"context"
	"fmt"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/mongo"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type initHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibInitHandler(env types.Environment) types.FuncHandler {
	return &initHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoInitHandler(env types.Environment) types.FuncHandler {
	return &initHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func initSlib(ctx context.Context, env types.Environment) error {
	store := statestore.CreateEnv(ctx, env)

	if result := store.Object("timeline").MakeArray("posts", 0); result.Err != nil {
		return result.Err
	}

	if result := store.Object("next_user_id").SetNumber("value", 0); result.Err != nil {
		return result.Err
	}

	return nil
}

func initMongo(ctx context.Context, client *mongo.Client) error {
	// db := client.Database("retwis")

	// if err := utils.MongoCreateCounter(ctx, db, "next_user_id"); err != nil {
	// 	return err
	// }

	// if err := utils.MongoCreateIndex(ctx, db.Collection("users"), "userId", true /* unique */); err != nil {
	// 	return err
	// }

	// if err := utils.MongoCreateIndex(ctx, db.Collection("users"), "username", true /* unique */); err != nil {
	// 	return err
	// }

	// return nil

	db := CreateMysqlClientOrDie(ctx)

	fmt.Println(db)
	fmt.Println(err)

	db.QueryContext(ctx, "DROP TABLE posts;")
	db.QueryContext(ctx, "DROP TABLE following;")
	db.QueryContext(ctx, "DROP TABLE logins;")
	db.QueryContext(ctx, "DROP TABLE users;")

	db.Query("CREATE TABLE IF NOT EXISTS users (userId int PRIMARY KEY, username varchar(255), password varchar(255), auth varchar(255),followers int, following int, posts int);");
	
	db.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS following ( followingUser int, followedUser int, FOREIGN KEY (followedUser) REFERENCES users(userId), FOREIGN KEY (followingUser) REFERENCES users(userId) );")

	db.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS posts(userID int, username varchar(255), post varchar(255), dt DATETIME, postId varchar(255), FOREIGN KEY (userID) REFERENCES users(userId));")

	db.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS logins ( userID int, dt DATETIME, successful BOOLEAN, FOREIGN KEY (userID) REFERENCES users(userId) )")

	//Created the database from the follwing-schema

	// CREATE TABLE users (
	// 	userId int PRIMARY KEY,
	// 	username varchar(255),
	// 	password varchar(255),
	//  auth varchar(255),
	//  followers int,
	//  following int,
	//  posts int,
	// );
	
	// CREATE TABLE following(
	// 	followingUser int,
	// 	followedUser int,
		
	// 	FOREIGN KEY (followedUser) REFERENCES users(userId),
	// 	FOREIGN KEY (followingUser) REFERENCES users(userId)
	// );
	
	// CREATE TABLE posts (
	// 	userID int, 
	// 	post varchar(255),
	// 	dt DATETIME,
	//  postId varchar(255),
	// 	FOREIGN KEY (userID) REFERENCES users(userId)
	// )

	// CREATE TABLE logins (
	// 	userID int,
	// 	dt DATETIME,
	// 	successful BOOLEAN,

	// 	FOREIGN KEY (userID) REFERENCES users(userId)
	// )
}

func (h *initHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	var err error
	switch h.kind {
	case "slib":
		err = initSlib(ctx, h.env)
	case "mongo":
		err = initMongo(ctx, h.client)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}

	if err != nil {
		return nil, err
	} else {
		return []byte("Init done\n"), nil
	}
}
