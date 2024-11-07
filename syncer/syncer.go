package syncer

import (
    "context"
    "fmt"
    "sync"

    "github.com/sirupsen/logrus"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    "mongodb_sync/config"
)

type Syncer struct {
    clientA      *mongo.Client
    clientB      *mongo.Client
    syncMappings []config.SyncMapping
    logger       *logrus.Logger
}

func NewSyncer(clientA, clientB *mongo.Client, syncMappings []config.SyncMapping, logger *logrus.Logger) *Syncer {
    return &Syncer{
        clientA:      clientA,
        clientB:      clientB,
        syncMappings: syncMappings,
        logger:       logger,
    }
}

func (s *Syncer) Start(ctx context.Context) {
    var wg sync.WaitGroup

    for _, mapping := range s.syncMappings {
        wg.Add(1)
        go func(mapping config.SyncMapping) {
            defer wg.Done()
            s.syncDatabase(ctx, mapping)
        }(mapping)
    }

    // 等待所有同步协程完成
    go func() {
        wg.Wait()
        s.logger.Info("所有数据库的同步已完成")
    }()
}

func (s *Syncer) syncDatabase(ctx context.Context, mapping config.SyncMapping) {
    dbA := s.clientA.Database(mapping.SourceDatabase)
    dbB := s.clientB.Database(mapping.TargetDatabase)

    // 对需要同步的集合进行初始同步
    for _, collMapping := range mapping.Collections {
        collA := dbA.Collection(collMapping.SourceCollection)
        collB := dbB.Collection(collMapping.TargetCollection)

        err := s.initialSync(ctx, collA, collB, mapping.SourceDatabase, mapping.TargetDatabase, collMapping)
        if err != nil {
            s.logger.Errorf("初始同步集合 %s.%s 到 %s.%s 失败：%v",
                mapping.SourceDatabase, collMapping.SourceCollection,
                mapping.TargetDatabase, collMapping.TargetCollection, err)
            // 初始同步失败，跳过该集合
            continue
        }
    }

    // 创建数据库级别的 Change Stream
    pipeline := mongo.Pipeline{}
    opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

    cs, err := dbA.Watch(ctx, pipeline, opts)
    if err != nil {
        s.logger.Errorf("监听数据库 %s 的 Change Stream 失败：%v", mapping.SourceDatabase, err)
        return
    }
    defer cs.Close(ctx)

    s.logger.Infof("开始监听数据库 %s 的变更", mapping.SourceDatabase)

    for cs.Next(ctx) {
        var changeEvent bson.M
        if err := cs.Decode(&changeEvent); err != nil {
            s.logger.Errorf("解码变更事件失败：%v", err)
            continue
        }

        // 获取集合名称
        ns, ok := changeEvent["ns"].(bson.M)
        if !ok {
            s.logger.Warn("无法获取命名空间信息")
            continue
        }
        collectionName, ok := ns["coll"].(string)
        if !ok {
            s.logger.Warn("无法获取集合名称")
            continue
        }

        // 查找对应的集合映射
        collMapping, found := s.getCollectionMapping(mapping.Collections, collectionName)
        if !found {
            // 非目标集合，跳过
            continue
        }

        // 获取对应的目标集合
        collB := dbB.Collection(collMapping.TargetCollection)

        // 处理变更事件
        s.handleChangeEvent(ctx, changeEvent, collB, mapping.TargetDatabase, collMapping.TargetCollection)
    }

    if err := cs.Err(); err != nil {
        s.logger.Errorf("Change Stream 错误：%v", err)
    }
}

func (s *Syncer) getCollectionMapping(collections []config.CollectionMapping, sourceCollectionName string) (config.CollectionMapping, bool) {
    for _, mapping := range collections {
        if mapping.SourceCollection == sourceCollectionName {
            return mapping, true
        }
    }
    return config.CollectionMapping{}, false
}

func (s *Syncer) handleChangeEvent(ctx context.Context, changeEvent bson.M, collB *mongo.Collection, targetDatabase, targetCollection string) {
    operationType, _ := changeEvent["operationType"].(string)
    fullDocument := changeEvent["fullDocument"]
    documentKey, _ := changeEvent["documentKey"].(bson.M)
    filter := documentKey

    switch operationType {
    case "insert":
        s.logger.Infof("处理插入事件，集合：%s.%s，文档：%v", targetDatabase, targetCollection, fullDocument)
        if fullDocument != nil {
            _, err := collB.InsertOne(ctx, fullDocument)
            if err != nil {
                s.logger.Errorf("插入文档到集合 %s.%s 失败：%v",
                    targetDatabase, targetCollection, err)
            } else {
                s.logger.Infof("插入文档到集合 %s.%s 成功",
                    targetDatabase, targetCollection)
            }
        }
    case "update", "replace":
        s.logger.Infof("处理更新事件，集合：%s.%s，文档：%v", targetDatabase, targetCollection, fullDocument)
        if fullDocument != nil {
            _, err := collB.ReplaceOne(ctx, filter, fullDocument)
            if err != nil {
                s.logger.Errorf("更新集合 %s.%s 中的文档失败：%v",
                    targetDatabase, targetCollection, err)
            } else {
                s.logger.Infof("更新集合 %s.%s 中的文档成功",
                    targetDatabase, targetCollection)
            }
        }
    case "delete":
        s.logger.Infof("处理删除事件，集合：%s.%s，文档Key：%v", targetDatabase, targetCollection, documentKey)
        _, err := collB.DeleteOne(ctx, filter)
        if err != nil {
            s.logger.Errorf("删除集合 %s.%s 中的文档失败：%v",
                targetDatabase, targetCollection, err)
        } else {
            s.logger.Infof("删除集合 %s.%s 中的文档成功",
                targetDatabase, targetCollection)
        }
    default:
        s.logger.Warnf("未处理的操作类型：%s", operationType)
    }
}

func (s *Syncer) initialSync(ctx context.Context, collA, collB *mongo.Collection, sourceDatabase, targetDatabase string, collMapping config.CollectionMapping) error {
    // 清空目标集合（根据需要，可以选择不清空）
    err := collB.Drop(ctx)
    if err != nil {
        return fmt.Errorf("清空目标集合失败：%v", err)
    }

    // 从源集合读取所有文档
    cursor, err := collA.Find(ctx, bson.M{})
    if err != nil {
        return fmt.Errorf("查询源集合失败：%v", err)
    }
    defer cursor.Close(ctx)

    var documents []interface{}
    for cursor.Next(ctx) {
        var doc bson.M
        if err := cursor.Decode(&doc); err != nil {
            return fmt.Errorf("解码文档失败：%v", err)
        }
        documents = append(documents, doc)
    }

    if err := cursor.Err(); err != nil {
        return fmt.Errorf("遍历游标时出错：%v", err)
    }

    if len(documents) > 0 {
        // 将文档批量插入到目标集合
        _, err = collB.InsertMany(ctx, documents)
        if err != nil {
            return fmt.Errorf("插入文档到目标集合失败：%v", err)
        }
    }

    s.logger.Infof("初始同步集合 %s.%s 到 %s.%s 完成，共同步 %d 个文档",
        sourceDatabase, collMapping.SourceCollection,
        targetDatabase, collMapping.TargetCollection, len(documents))
    return nil
}
