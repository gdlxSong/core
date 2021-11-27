## Resources

我们为什么需要去定义 `Resources`， 这个 idle 源自我们需要对对 `提供某种特定功能类型的中间件` 进行封装，屏蔽底层实现，以满足不同用户的需求。







关于 core 可能使用的资源类型： `MQ | SearchEngine | RelationalDB | TSDB | GraphDB`.



每一个 Resource 都是一个独立的资源片， 每一个实体都属于一个特定的 Namespace， Namespace 里面可以配置特定的 Resource.

Namespace 和 Subscription的存在形式应该是相同的， 都应该被归属于 `meta data` 的集合中, 如此，我们可以实现通过Namespace的资源隔离.



