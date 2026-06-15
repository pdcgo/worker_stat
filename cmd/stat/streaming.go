package main

// type StreamingFunc cli.ActionFunc

// func NewStreaming(
// 	cfg *configs.AppConfig,
// 	db *gorm.DB,
// ) StreamingFunc {
// 	return func(ctx context.Context, c *cli.Command) error {
// 		var err error
// 		if c.Bool("debug") {
// 			slog.SetLogLoggerLevel(slog.LevelDebug)
// 		}

// 		// initialize trace
// 		cancelTrace, err := custom_connect.InitTracer("stock_updater")
// 		if err != nil {
// 			return err
// 		}

// 		defer cancelTrace(ctx)

// 		var schema string = "test"

// 		stream := streaming_compute.NewStreamingContext(
// 			streaming_compute.WithSchemaOption(schema),
// 		)

// 		// registering source
// 		err = stream.RegisterSource(
// 			db,
// 			&streaming_metric.InvTransactionChange{},
// 		)
// 		if err != nil {
// 			return err
// 		}

// 		err = stream.RegisterSink(
// 			db,
// 			&streaming_metric.SkuStock{},
// 		)
// 		if err != nil {
// 			return err
// 		}

// 		// registering computation
// 		compute := stream.Compute(
// 			&streaming_metric.SkuStock{},
// 			// &streaming_metric.VariantStock{},
// 		)

// 		// generate visualization
// 		visual := c.String("visualization")
// 		if visual != "" {
// 			err = stream.GenerateVisualization(visual)
// 			if err != nil {
// 				return err
// 			}
// 		}

// 		// create replication context
// 		replicate, err := replication.ConnectReplication(ctx, &cfg.Database)

// 		if err != nil {
// 			return err
// 		}

// 		process := common_helper.NewChainParam(
// 			func(next common_helper.NextFuncParam[*replication.ReplicationEvent]) common_helper.NextFuncParam[*replication.ReplicationEvent] {
// 				return func(event *replication.ReplicationEvent) (*replication.ReplicationEvent, error) { // filtering cuma inv_transaction
// 					switch event.SourceMetadata.Table {
// 					case "inv_transactions":
// 						return next(event)
// 					default:
// 						return event, nil
// 					}
// 				}
// 			},
// 			func(next common_helper.NextFuncParam[*replication.ReplicationEvent]) common_helper.NextFuncParam[*replication.ReplicationEvent] {
// 				return func(data *replication.ReplicationEvent) (*replication.ReplicationEvent, error) {
// 					var err error
// 					stream.Lock()
// 					defer stream.Unlock()

// 					id, ok := data.Data["id"].(int64)
// 					if !ok {
// 						return data, errors.New("cannot get id")
// 					}

// 					tx_type := data.Data["type"].(string)
// 					status := data.Data["status"].(string)

// 					change := streaming_metric.InvTransactionChange{
// 						At:      time.Now().UnixMicro(),
// 						TxID:    uint64(id),
// 						ModType: data.ModType,
// 						TxType:  db_models.InvTxType(tx_type),
// 						Status:  db_models.InvTxStatus(status),
// 					}

// 					slog.Debug("add item to source",
// 						slog.Any("change", change),
// 					)

// 					err = stream.EmitToSource(db, &change)
// 					if err != nil {
// 						return data, err
// 					}
// 					return data, err
// 				}
// 			},
// 		)

// 		// creating runner
// 		rctx := streaming_compute.NewRunnerContext(ctx)

// 		streaming_compute.Run(
// 			"replication",
// 			rctx,
// 			func() error {
// 				return replicate.
// 					StreamStart(
// 						rctx,
// 						"test",
// 						"stat_publication",
// 						func(ctx context.Context, event *replication.ReplicationEvent) error {
// 							_, err = process(event)
// 							return err
// 						},
// 					)
// 			},
// 		)

// 		// log.Println(compute)

// 		streaming_compute.RunPeriodically(
// 			"periodic_stream",
// 			rctx,
// 			time.Second*10,
// 			func() error {
// 				err = db.
// 					Transaction(func(tx *gorm.DB) error {
// 						return compute(rctx, tx)
// 					},
// 						&sql.TxOptions{
// 							Isolation: sql.LevelRepeatableRead,
// 						},
// 					)
// 				slog.Info("finished")
// 				return err
// 			},
// 		)

// 		<-rctx.Done()

// 		slog.Info("replication existed")

// 		err = rctx.
// 			Error

// 		if err != nil {
// 			slog.Error(err.Error())
// 		}
// 		return err
// 	}
// }

// type SkuOngoingStock struct{}

// // BuildQuery implements [batch_compute.Table].
// func (s SkuOngoingStock) BuildQuery(graph *batch_compute.GraphContext) string {
// 	return `

// 	`
// }

// // TableName implements [batch_compute.Table].
// func (s SkuOngoingStock) TableName() string {
// 	return "sku_ongoing_stock"
// }

// // Temporary implements [batch_compute.Table].
// func (s SkuOngoingStock) Temporary() bool {
// 	return false
// }

// type IncrementalTable struct {
// 	table batch_compute.Table
// }

// // BuildQuery implements [batch_compute.Table].
// func (i *IncrementalTable) BuildQuery(graph *batch_compute.GraphContext) string {
// 	return `

// 	`
// }

// // TableName implements [batch_compute.Table].
// func (i *IncrementalTable) TableName() string {
// 	return fmt.Sprintf("%s_inc", i.table.TableName())
// }

// // Temporary implements [batch_compute.Table].
// func (i *IncrementalTable) Temporary() bool {
// 	return false
// }

// func (i *IncrementalTable) BuildQueries(graph *batch_compute.GraphContext) []string {
// 	queries := []string{}
// 	queries = graph.BuildQueries(i.table)

// 	tableName := graph.GetTableName(i.table)
// 	// copy table if not exists
// 	queries = append(queries,
// 		fmt.Sprintf(
// 			"create table if not exists %s.%s as\n %s",
// 			graph.Schema,
// 			tableName,
// 			i.table.BuildQuery(graph),
// 		),
// 	)

// 	return queries

// }

// func NewIncrementalTable(table batch_compute.Table) batch_compute.Table {
// 	return &IncrementalTable{table}
// }

// var cc batch_compute.Table = SkuOngoingStock{}
