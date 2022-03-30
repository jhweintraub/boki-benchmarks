package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	_ "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	_ "database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type ProfileInput struct {
	UserId string `json:"userId"`
}

type ProfileOutput struct {
	Success      bool   `json:"success"`
	Message      string `json:"message,omitempty"`
	UserName     string `json:"username,omitempty"`
	NumFollowers int    `json:"numFollowers"`
	NumFollowees int    `json:"numFollowees"`
	NumPosts     int    `json:"numPosts"`
}

type profileHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibProfileHandler(env types.Environment) types.FuncHandler {
	return &profileHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoProfileHandler(env types.Environment) types.FuncHandler {
	return &profileHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func profileSlib(ctx context.Context, env types.Environment, input *ProfileInput) (*ProfileOutput, error) {
	output := &ProfileOutput{Success: true}

	store := statestore.CreateEnv(ctx, env)
	userObj := store.Object(fmt.Sprintf("userid:%s", input.UserId))
	if value, _ := userObj.Get("username"); !value.IsNull() {
		output.UserName = value.AsString()
	} else {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("Cannot find user with ID %s", input.UserId),
		}, nil
	}
	if value, _ := userObj.Get("followers"); !value.IsNull() {
		output.NumFollowers = value.Size()
	}
	if value, _ := userObj.Get("followees"); !value.IsNull() {
		output.NumFollowees = value.Size()
	}
	if value, _ := userObj.Get("posts"); !value.IsNull() {
		output.NumPosts = value.Size()
	}

	return output, nil
}

func profileMongo(ctx context.Context, client *mongo.Client, input *ProfileInput) (*ProfileOutput, error) {
	// db := client.Database("retwis")

	db := utils.CreateMysqlClientOrDie(ctx)

	//Optimized one-query to get both count and information

	results, err := db.QueryContext(ctx, "SELECT userId, username, followers, following, posts, COUNT(*) FROM users where userId=?", input.UserId)

	type user struct {
		userId int
		username string
		followers int
		following int
		posts int
		count int
	}	

	var userInfo user

	//Loop through the results rows - There should only be one
	for results.Next() {
		results.Scan(&userInfo.userId, &userInfo.username, &userInfo.followers, &userInfo.following, &userInfo.posts, &userInfo.count)
	}

	//If it errored or there just wasn't the one row we're looking for
	if (userInfo.count != 1 || err != nil) {
		return &ProfileOutput{
			Success: false,
			Message: fmt.Sprintf("Mongo failed: %v %v", userInfo.count, err),
		}, nil
	}

	// var user bson.M

	//Check to see that user exists based on userId
	// if err := db.Collection("users").FindOne(ctx, bson.D{{"userId", input.UserId}}).Decode(&user); err != nil {
		// return &ProfileOutput{
		// 	Success: false,
		// 	Message: fmt.Sprintf("Mongo failed: %v", err),
		// }, nil
	// }

	output := &ProfileOutput{Success: true}

	//Acquire Username - Simple query based on UserID
	// if value, ok := user["username"].(string); ok {
	// 	output.UserName = value
	// }
	output.UserName = userInfo.username
	output.NumFollowers = userInfo.followers
	output.NumFollowees = userInfo.following
	output.NumPosts = userInfo.posts

	// //Acquire list of Followers - Potentially more complex query requiring count()
	// if value, ok := user["followers"].(bson.M); ok {
	// 	output.NumFollowers = len(value)
	// }

	// //Acquire list of people following - Potentially more complex query requiring count()
	// if value, ok := user["followees"].(bson.M); ok {
	// 	output.NumFollowees = len(value)
	// }

	// //Acquire number of posts
	// if value, ok := user["posts"].(bson.A); ok {
	// 	output.NumPosts = len(value)
	// }

	
	/*Prediction - MySQL is almost definitely going to be slower because it requires either 
	very complex queries, more tables, or more simple-queries and the additional time that takes is just
	going to be much longer 

	Question - Add another table for "metadata" that includes numbers of posts, followers, etc. - speeds up retrieval time
	but at the cost of insertion times */

	return output, nil
}

func (h *profileHandler) onRequest(ctx context.Context, input *ProfileInput) (*ProfileOutput, error) {
	switch h.kind {
	case "slib":
		return profileSlib(ctx, h.env, input)
	case "mongo":
		return profileMongo(ctx, h.client, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *profileHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ProfileInput{}
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
