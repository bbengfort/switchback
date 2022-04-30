package switchback

import (
	"errors"
	"sync"

	"github.com/bbengfort/switchback/pkg/api/v1"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type PubSub struct {
	sync.Mutex
	topics map[string]map[string]*Group
}

type Group struct {
	sync.RWMutex
	id        string
	consumers []*Consumer
	offset    uint64
	index     int
}

type Consumer struct {
	id     uuid.UUID
	stream api.Switchback_SubscribeServer
}

func (p *PubSub) Connect(sub *api.Subscription, stream api.Switchback_SubscribeServer) error {
	if sub.Group == "" {
		sub.Group = uuid.New().String()
	}

	p.Lock()
	defer p.Unlock()
	if _, ok := p.topics[sub.Topic]; !ok {
		p.topics[sub.Topic] = make(map[string]*Group)
	}

	if _, ok := p.topics[sub.Topic][sub.Group]; !ok {
		p.topics[sub.Topic][sub.Group] = &Group{
			id:        sub.Group,
			consumers: make([]*Consumer, 0, 1),
			offset:    0,
			index:     0,
		}
	}

	group := p.topics[sub.Topic][sub.Group]
	group.consumers = append(group.consumers, &Consumer{id: uuid.New(), stream: stream})
	return nil
}

func (p *PubSub) Publish(event *api.Event) (err error) {
	// TODO: don't simply drop event, wait for consumer to connect then emit event (queuing behavior)
	if groups, ok := p.topics[event.Topic]; ok {
		for _, group := range groups {
			// TODO: use multierror to return all group errors to the caller
			if err = group.Publish(event); err != nil {
				log.Error().Err(err).Str("topic", event.Topic).Str("group", group.id).Msg("could not publish event to group")
			}
		}
	}
	return nil
}

func (g *Group) Publish(event *api.Event) (err error) {
	g.RLock()
	defer g.RUnlock()

	if len(g.consumers) == 0 {
		return errors.New("no available consumers")
	}

	if g.index >= len(g.consumers) {
		g.index = 0
	}

	if err = g.consumers[g.index].stream.Send(event); err != nil {
		// TODO: If io.EOF then gracefully close this consumer and retry
		return err
	}

	g.index++
	if g.index >= len(g.consumers) {
		g.index = 0
	}
	return nil
}
