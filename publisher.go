package bus_golang_publisher_aws_kinesis

import (
	"context"
	"fmt"

	"github.com/al-kimmel-serj/bus-golang"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"
)

type Publisher[Payload proto.Message] struct {
	kinesisClient    *kinesis.Client
	kinesisStreamARN string
	unregister       func() error
}

func New[Payload proto.Message](
	host string,
	port int,
	eventName bus.EventName,
	eventVersion bus.EventVersion,
	publishersRegistry bus.PublishersRegistry,
	kinesisClient *kinesis.Client,
	kinesisStreamARN string,
) (*Publisher[Payload], error) {
	unregister, err := publishersRegistry.Register(eventName, eventVersion, host, port)
	if err != nil {
		return nil, fmt.Errorf("PublishersRegistry.Register error: %w", err)
	}

	return &Publisher[Payload]{
		kinesisClient:    kinesisClient,
		kinesisStreamARN: kinesisStreamARN,
		unregister:       unregister,
	}, nil
}

func (p *Publisher[Payload]) Publish(ctx context.Context, events []bus.Event[Payload]) error {
	if len(events) == 0 {
		return nil
	}

	records := make([]types.PutRecordsRequestEntry, 0, len(events))
	for _, event := range events {
		msg, err := proto.Marshal(event.EventPayload)
		if err != nil {
			return fmt.Errorf("proto.Marshal error: %w", err)
		}

		records = append(records, types.PutRecordsRequestEntry{
			Data:         msg,
			PartitionKey: (*string)(&event.EventKey),
		})
	}

	_, err := p.kinesisClient.PutRecords(ctx, &kinesis.PutRecordsInput{
		Records:   records,
		StreamARN: &p.kinesisStreamARN,
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Publisher[Payload]) Stop() error {
	err := p.unregister()
	if err != nil {
		return fmt.Errorf("unregister error: %w", err)
	}

	return nil
}
