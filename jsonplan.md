```json
{
	"jid": "0ec9f2def5f612245f7c6d980e63c5c5",
	"name": "Socket Window WordCount",
	"nodes": [{
		"id": "6d2677a0ecc3fd8df0b72ec675edf8f4",
		"parallelism": 1,
		"operator": "",
		"operator_strategy": "",
		"description": "Sink: Print to Std. Out",
		"inputs": [{
			"num": 0,
			"id": "ea632d67b7d595e5b851708ae9ad79d6",
			"ship_strategy": "REBALANCE",
			"exchange": "pipelined_bounded"
		}],
		"optimizer_properties": {}
	}, {
		"id": "ea632d67b7d595e5b851708ae9ad79d6",
		"parallelism": 4,
		"operator": "",
		"operator_strategy": "",
		"description": "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction)",
		"inputs": [{
			"num": 0,
			"id": "0a448493b4782967b150582570326227",
			"ship_strategy": "HASH",
			"exchange": "pipelined_bounded"
		}],
		"optimizer_properties": {}
	}, {
		"id": "0a448493b4782967b150582570326227",
		"parallelism": 4,
		"operator": "",
		"operator_strategy": "",
		"description": "Flat Map",
		"inputs": [{
			"num": 0,
			"id": "bc764cd8ddf7a0cff126f51c16239658",
			"ship_strategy": "REBALANCE",
			"exchange": "pipelined_bounded"
		}],
		"optimizer_properties": {}
	}, {
		"id": "bc764cd8ddf7a0cff126f51c16239658",
		"parallelism": 1,
		"operator": "",
		"operator_strategy": "",
		"description": "Source: Socket Stream",
		"optimizer_properties": {}
	}]
}
```
