package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"cs.utexas.edu/zjia/faas-retwis/utils"

	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type RegisterInput struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type RegisterOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	UserId  string `json:"userId"`
}

type registerHandler struct {
	kind   string
	env    types.Environment
	client *mongo.Client
}

func NewSlibRegisterHandler(env types.Environment) types.FuncHandler {
	return &registerHandler{
		kind: "slib",
		env:  env,
	}
}

func NewMongoRegisterHandler(env types.Environment) types.FuncHandler {
	return &registerHandler{
		kind:   "mongo",
		env:    env,
		client: utils.CreateMongoClientOrDie(context.TODO()),
	}
}

func registerSlib(ctx context.Context, env types.Environment, input *RegisterInput) (*RegisterOutput, error) {
	store := statestore.CreateEnv(ctx, env)
	nextUserIdObj := store.Object("next_user_id")
	result := nextUserIdObj.NumberFetchAdd("value", 1)
	if result.Err != nil {
		return nil, result.Err
	}
	userIdValue := uint32(result.Value.AsNumber())

	txn, err := statestore.CreateTxnEnv(ctx, env)
	if err != nil {
		return nil, err
	}

	userNameObj := txn.Object(fmt.Sprintf("username:%s", input.UserName))
	if value, _ := userNameObj.Get("id"); !value.IsNull() {
		txn.TxnAbort()
		return &RegisterOutput{
			Success: false,
			Message: fmt.Sprintf("User name \"%s\" already exists", input.UserName),
		}, nil
	}

	userId := fmt.Sprintf("%08x", userIdValue)
	userNameObj.SetString("id", userId)

	userObj := txn.Object(fmt.Sprintf("userid:%s", userId))
	userObj.SetString("username", input.UserName)
	userObj.SetString("password", input.Password)
	userObj.SetString("auth", fmt.Sprintf("%016x", rand.Uint64()))
	userObj.MakeObject("followers")
	userObj.MakeObject("followees")
	userObj.MakeArray("posts", 0)

	if committed, err := txn.TxnCommit(); err != nil {
		return nil, err
	} else if committed {
		return &RegisterOutput{
			Success: true,
			UserId:  userId,
		}, nil
	} else {
		return &RegisterOutput{
			Success: false,
			Message: "Failed to commit transaction due to conflicts",
		}, nil
	}
}

func registerMongo(ctx context.Context, client *mongo.Client, input *RegisterInput) (*RegisterOutput, error) {
	sess, err := client.StartSession(options.Session())
	if err != nil {
		return nil, err
	}
	defer sess.EndSession(ctx)

	userId, err := sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		
		//connect to the database

		// db := client.Database("retwis")
		db := utils.mysqlFetchAddCounter(sessCtx)
		userIdValue, err := utils.mysqlFetchAddCounter(sessCtx, db, 1)

		//Need to override this here to get the next userIdValue

		// userIdValue, err := utils.MongoFetchAddCounter(sessCtx, db, "next_user_id", 1)
		if err != nil {
			return nil, err
		}

		userId := fmt.Sprintf("%08x", userIdValue)
		auth :=  fmt.Sprintf("%016x", rand.Uint64())} //A series of hex values - represented as varchar I guess


		results, err := db.QueryContext(ctx, "INSERT INTO users(userId, username, password, auth) VALUES(?, ?, ?, ?)", userId, input.UserName, input.Password, auth)

		//Null check for query execution - TODO - Make code cleaner
		if err != nil {
			return nil, err
		}

		results,err := db.QueryContext("SELECT userId FROM users where username=?", input.UserName)

		if err != nil {
			fmt.Println("FAILURE IN EXECUTING QUERY")
		}
	
		
	
		for results.Next() {
			results.Scan(&id)
			
		}

		if userId != id {
			//Check to make sure that the values were inserted correctly, otherwise return error
			return nil, err
		}

		

		// userId := fmt.Sprintf("%08x", userIdValue)
		// userBson := bson.D{
		// 	{"userId", userId},
		// 	{"username", input.UserName},
		// 	{"password", input.Password},

		// ------- AUTH = Random string as Auth Token ---
	
		// 	{"auth", fmt.Sprintf("%016x", rand.Uint64())},
		// 	{"followers", bson.D{}},
		// 	{"followees", bson.D{}},
		// 	{"posts", bson.A{}},
		// }


		//Insert and if failure return the error.
		// if _, err := db.Collection("users").InsertOne(sessCtx, userBson); err != nil {
		// 	return nil, err
		// }

		
			//Otherwise return the new userID
		return userId, nil
	}, utils.MongoTxnOptions())

	if err != nil {
		return &RegisterOutput{
			Success: false,
			Message: fmt.Sprintf("Mysql Insertion failed: %v", err),
		}, nil
	}

	return &RegisterOutput{
		Success: true,

		//Guessing this is like a toString() type-method but not entirely sure
		UserId:  userId.(string),
	}, nil
}

func (h *registerHandler) onRequest(ctx context.Context, input *RegisterInput) (*RegisterOutput, error) {
	switch h.kind {
	case "slib":
		return registerSlib(ctx, h.env, input)
	case "mongo":
		return registerMongo(ctx, h.client, input)
	default:
		panic(fmt.Sprintf("Unknown kind: %s", h.kind))
	}
}

func (h *registerHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &RegisterInput{}
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
