package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	switchback "github.com/bbengfort/switchback/pkg"
	"github.com/bbengfort/switchback/pkg/api/v1"
	"github.com/bbengfort/switchback/pkg/config"
	"github.com/joho/godotenv"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Load the dotenv file if it exists
	godotenv.Load()

	// Create the CLI application
	app := &cli.App{
		Name:    "sbs",
		Version: switchback.Version(),
		Usage:   "a pub/sub server for simple eventing interactions",
		Flags:   []cli.Flag{},
		Commands: []*cli.Command{
			{
				Name:     "serve",
				Usage:    "serves the switchback server",
				Category: "server",
				Action:   serve,
				Flags:    []cli.Flag{},
			},
			{
				Name:     "status",
				Usage:    "get the status of a running switchback server",
				Category: "server",
				Action:   status,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "endpoint",
						Aliases: []string{"e"},
						Usage:   "the endpoint to connect to the switchback server on",
						Value:   "localhost:7773",
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func serve(c *cli.Context) (err error) {
	var conf config.Config
	if conf, err = config.New(); err != nil {
		return cli.Exit(err, 1)
	}

	var srv *switchback.Server
	if srv, err = switchback.New(conf); err != nil {
		return cli.Exit(err, 1)
	}

	if err = srv.Serve(); err != nil {
		return cli.Exit(err, 1)
	}
	return nil
}

func status(c *cli.Context) (err error) {
	var cc *grpc.ClientConn
	if cc, err = grpc.Dial(c.String("endpoint"), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return cli.Exit(err, 1)
	}

	defer cc.Close()
	client := api.NewSwitchbackClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var rep *api.ServiceState
	if rep, err = client.Status(ctx, &api.HealthCheck{}); err != nil {
		return cli.Exit(err, 1)
	}
	return printJSON(rep)
}

func printJSON(msg interface{}) (err error) {
	var data []byte
	switch m := msg.(type) {
	case proto.Message:
		opts := protojson.MarshalOptions{
			Multiline:       true,
			Indent:          "  ",
			AllowPartial:    true,
			UseProtoNames:   true,
			UseEnumNumbers:  false,
			EmitUnpopulated: true,
		}

		if data, err = opts.Marshal(m); err != nil {
			return cli.Exit(err, 1)
		}
	default:
		if data, err = json.MarshalIndent(msg, "", "  "); err != nil {
			return cli.Exit(err, 1)
		}
	}

	fmt.Println(string(data))
	return nil
}
