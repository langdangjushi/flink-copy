### StreamExecutionEnviroment

* chaining enabled
* statebackend



### DataStream

* StreamExecutionEnviroment
* StreamTransformation

### StreamOperator

* user function

### StreamTransform

* Input stransformation
* name
* StreamOperator
* output type
* parallelism  == streamExecutionEnviroment.parallelism

### StreamNode  generatre from transformation

* vertexId
* slotSharingGroup
* coLocationGropu
* vertexClass
* name
* output selector
* parallelism
* max parallelism
* input serializer
* output serializer
* StreamEdge
* out edge
* in edge

### StreamEdge

* upStreamNode
* downStreamNode
* partitioner
* outputTag
* outputPartitioner

### JobVertex

* name
* id
* operatorIds
* invokableClass  == headNode.jobvertexclass
* parrallelism = headNode.parallelism
* maxParallelism
* inputs --> jobEdges
* reults -> ArrayList<IntermediateDataSet>

### IntermediateDataSet

* producer  --> Jobvertex
* consumers  --> jobedge
* partitiontype

### JobEdge

* source  --- IntermediateDataSet
* target -- -JobVertex
* distributionPattern
* shipstrategy
* results =--> intermediateDataset

### IntermediateResult

* id
* producer --> ExecutionJobVertex
* numtaskvertices  就是  numParallelProducers
* numParallelProducers
* partions  --> IntermediateResultPartition

### ExecutionJobVertex

* exec graph
* jobvertex
* parallelism
* globalmodversion
* ExecutionVertex  -- list
* inputs  -- list of intermediate result
* producedDataSets  ---> IntermediateResult 当天jobvertex的 ISt的数量个
* taskVertices --> list of ExecutionVertex

### ExecutionVertex

* JobVertex
* index
* producedDataSets
* resultPartions
* inputEdges

### ExecutionEdge

* source ---> IntermediateResultPartition
* target --> ExecutionVertex
* inputNum 

### IntermediateResultPartition

* result ---> which intermediate this result belongs to 
* producer --> ExecutionVertex
* subtask index
