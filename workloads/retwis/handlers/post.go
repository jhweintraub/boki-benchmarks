package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	_ "go.mongodb.org/mongo-driver/bson"
	_ "go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	_ "go.mongodb.org/mongo-driver/mongo/options"

	"crypto/md5"
	"encoding/hex"
	"time"

	_ "database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type PostInput struct {
	UserId string `json:"userId"`
	Body   string `json:"body"`
}

type PostOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type postHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibPostHandler(env types.Environment) types.FuncHandler {
	return &postHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoPostHandler(env types.Environment) types.FuncHandler {
	return &postHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

const kMaxNotifyUsers = 4
const kUserPostListLimit = 24
const kTimeLinePostListLimit = 96

func postSlib(ctx context.Context, env types.Environment, input *PostInput) (*PostOutput, error) {
	txn, err := statestore.CreateTxnEnv(ctx, env)
	if err != nil {
		return nil, err
	}

	userObj := txn.Object(fmt.Sprintf("userid:%s", input.UserId))
	userName := ""
	if value, _ := userObj.Get("username"); !value.IsNull() {
		userName = value.AsString()
	} else {
		txn.TxnAbort()
		return &PostOutput{
			Success: false,
			Message: fmt.Sprintf("Cannot find user with ID %s", input.UserId),
		}, nil
	}

	postId := fmt.Sprintf("%016x", env.GenerateUniqueID())
	postObj := txn.Object(fmt.Sprintf("post:%s", postId))
	postObj.SetString("id", postId)
	postObj.SetString("userId", input.UserId)
	postObj.SetString("userName", userName)
	postObj.SetString("body", input.Body)

	if value, _ := userObj.Get("followers"); !value.IsNull() && value.Size() > 0 {
		followers := make([]string, 0, 4)
		for follower, _ := range value.AsObject() {
			followers = append(followers, follower)
		}
		rand.Shuffle(len(followers), func(i, j int) {
			followers[i], followers[j] = followers[j], followers[i]
		})
		if len(followers) > kMaxNotifyUsers {
			followers = followers[0:kMaxNotifyUsers]
		}
		for _, follower := range followers {
			followUserObj := txn.Object(fmt.Sprintf("userid:%s", follower))
			followUserObj.ArrayPushBackWithLimit("posts", statestore.StringValue(postId), kUserPostListLimit)
		}
	}

	if committed, err := txn.TxnCommit(); err != nil {
		return nil, err
	} else if !committed {
		return &PostOutput{
			Success: false,
			Message: "Failed to commit transaction due to conflicts",
		}, nil
	}

	store := statestore.CreateEnv(ctx, env)
	timelineObj := store.Object("timeline")
	result := timelineObj.ArrayPushBackWithLimit("posts", statestore.StringValue(postId), kTimeLinePostListLimit)
	if result.Err != nil {
		return nil, result.Err
	}

	return &PostOutput{Success: true}, nil
}

func postMongo(ctx context.Context, client *mongo.Client, input *PostInput) (*PostOutput, error) {
	// sess, err := client.StartSession(options.Session())
	// if err != nil {
	// 	return nil, err
	// }
	// defer sess.EndSession(ctx)

	// db := client.Database("retwis")

	db := utils.CreateMysqlClientOrDie(ctx)

	// _, err = sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// postColl := db.Collection("posts")
		// usersColl := db.Collection("users")

		//Find user info - we need the username
		// var user bson.M
		// if err := usersColl.FindOne(sessCtx, bson.D{{"userId", input.UserId}}).Decode(&user); err != nil {
		// 	return nil, err
		// }

		//Get username
	profileResults, err1 := db.QueryContext(ctx, "SELECT username FROM users where userId=?", input.UserId)

	var username string

	for profileResults.Next() {
		profileResults.Scan(&username)
	}

	currentTime := time.Now()
	data := []byte(input.Body + input.UserId + currentTime.String())
	hash := md5.Sum(data)
	hashSum := hex.EncodeToString(hash[:10])

	//Insert into posts table
	_, err2 := db.QueryContext(ctx, "INSERT INTO posts values((SELECT userId from users where userId=?), (SELECT username from users where userId=?), ? , NOW(), ?);", input.UserId, input.UserId, input.Body, hashSum)


	//Update the number of posts for a user-object
	_, err3 := db.QueryContext(ctx, "UPDATE users SET posts = posts + 1 WHERE userId=?", input.UserId)

		//Post info
		// postBson := bson.D{
		// 	{"userId", input.UserId},
		// 	{"userName", user["username"].(string)},
		// 	{"body", input.Body},
		// }

		//Insert the above info into the database
		// var postId primitive.ObjectID
		// if result, err := postColl.InsertOne(sessCtx, postBson); err != nil {
		// 	return nil, err
		// } else {
		// 	postId = result.InsertedID.(primitive.ObjectID)
		// }

		//Pushes the post info to all the followers so all posts from people they subscribe to can be found
		//I think I can remove this because i'm overriding the other functionality this relies on

		// if value, ok := user["followers"].(bson.M); ok {
		// 	followers := make([]string, 0, 4)
		// 	for follower, _ := range value {
		// 		followers = append(followers, follower)
		// 	}
		// 	rand.Shuffle(len(followers), func(i, j int) {
		// 		followers[i], followers[j] = followers[j], followers[i]
		// 	})
		// 	if len(followers) > kMaxNotifyUsers {
		// 		followers = followers[0:kMaxNotifyUsers]
		// 	}
		// 	update := bson.M{
		// 		"$push": bson.M{
		// 			"posts": bson.M{
		// 				"$each":  bson.A{postId},
		// 				"$slice": -kUserPostListLimit,
		// 			},
		// 		},
		// 	}
		// 	for _, follower := range followers {
		// 		_, err := usersColl.UpdateOne(sessCtx, bson.D{{"userId", follower}}, update)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 	}
		// }

		// return nil, nil
	// }, utils.MongoTxnOptions())

	if (err1 != nil || err2 != nil || err3 != nil) {
		return &PostOutput{
			Success: false,
			Message: fmt.Sprintf("Mysql Updates failed: %v %v %v", err1, err2, err3),
		}, nil
	}

	return &PostOutput{Success: true}, nil
}

func (h *postHandler) onRequest(ctx context.Context, input *PostInput) (*PostOutput, error) {
	switch h.kind {
	case "slib":
		return postSlib(ctx, h.env, input)
	case "mongo":
		return postMongo(ctx, h.client, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *postHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &PostInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output, err := h.onRequest(ctx, parsedInput)
	if err != nil {
		return nil, err
	}
	return json.Marshal(output)
}
