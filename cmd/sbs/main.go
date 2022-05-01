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
				Category: "client",
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
			{
				Name:     "sub",
				Usage:    "print events from the stream as they come in",
				Category: "client",
				Action:   subscribe,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "endpoint",
						Aliases: []string{"e"},
						Usage:   "the endpoint to connect to the switchback server on",
						Value:   "localhost:7773",
					},
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Usage:   "the topic to subscribe to events for",
						Value:   "default",
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "the group the client is a part of",
					},
				},
			},
			{
				Name:     "random",
				Usage:    "randomly generate events in the specified topic and publish them",
				Category: "simulator",
				Action:   simulator,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "endpoint",
						Aliases: []string{"e"},
						Usage:   "the endpoint to connect to the switchback server on",
						Value:   "localhost:7773",
					},
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Usage:   "the topic to generate events on",
						Value:   "default",
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

func subscribe(c *cli.Context) (err error) {
	var cc *grpc.ClientConn
	if cc, err = grpc.Dial(c.String("endpoint"), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return cli.Exit(err, 1)
	}

	defer cc.Close()
	client := api.NewSwitchbackClient(cc)

	req := &api.Subscription{
		Topic: c.String("topic"),
		Group: c.String("group"),
	}

	var stream api.Switchback_SubscribeClient
	if stream, err = client.Subscribe(context.Background(), req); err != nil {
		return cli.Exit(err, 1)
	}

	for {
		var event *api.Event
		if event, err = stream.Recv(); err != nil {
			return cli.Exit(err, 1)
		}

		if err = printJSON(event); err != nil {
			return cli.Exit(err, 1)
		}
	}
}

func simulator(c *cli.Context) (err error) {
	var cc *grpc.ClientConn
	if cc, err = grpc.Dial(c.String("endpoint"), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return cli.Exit(err, 1)
	}

	defer cc.Close()
	client := api.NewSwitchbackClient(cc)

	var stream api.Switchback_PublishClient
	if stream, err = client.Publish(context.Background()); err != nil {
		return cli.Exit(err, 1)
	}

	topic := c.String("topic")
	ticker := time.NewTicker(2500 * time.Millisecond)

	for {
		ts := <-ticker.C
		event := &api.Event{Topic: topic, Data: []byte(ts.Format(time.RFC1123Z))}
		if err = stream.Send(event); err != nil {
			return cli.Exit(err, 1)
		}
	}
	return nil
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
