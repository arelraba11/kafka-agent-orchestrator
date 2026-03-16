Demo Scenarios — Kafka AI Agent (Final Project)

These scenarios demonstrate the end-to-end event-sourced pipeline.
All commands assume the repository root as the working directory.

Prerequisites
-------------
    docker-compose -f infra/docker-compose.yml up -d
    bash infra/topics.sh
    bun install
    bash scripts/start-final.sh
    bun run services/core/userInterface.ts   # separate terminal

Log files are in scripts/logs/final-project-services/.
Tail any log while typing in the UI:

    tail -f scripts/logs/final-project-services/router.log
    tail -f scripts/logs/final-project-services/orchestrator.log
    tail -f scripts/logs/final-project-services/weather.log
    tail -f scripts/logs/final-project-services/exchange.log
    tail -f scripts/logs/final-project-services/answer.log

-------------------------------------------------------------------------------
Scenario 1 — Single-step: Weather Request
-------------------------------------------------------------------------------

User input
----------
    weather in tel aviv

Expected plan
-------------
    { "steps": ["weather"] }

Event flow
----------
    UserQueryReceived       →  user-commands
    PlanGenerated           →  conversation-events
    ToolInvocationRequested →  tool-invocation-requests
    ToolInvocationResulted  →  conversation-events
    PlanCompleted           →  conversation-events
    FinalAnswerSynthesized  →  conversation-events  →  UI

Expected log sequence
---------------------

router.log:
    [router] conversationId=<id> plan=[weather] input="weather in tel aviv"
    [Benchmark] conversationId=<id> routerLatency=Xms

orchestrator.log:
    [orchestrator] conversationId=<id> plan received steps=[weather]
    [orchestrator] conversationId=<id> dispatching tool="weather"
    [orchestrator] conversationId=<id> step 1/1 completed tool="weather"
    [orchestrator] conversationId=<id> plan completed, results=1
    [Benchmark] conversationId=<id> workerLatency=Xms

weather.log:
    [weather] tool conversationId=<id> city="tel aviv"
    [weather] tool result="The weather in Tel Aviv is 28°C and Sunny."

answer.log:
    [synthesizer] conversationId=<id> results=1
    [synthesizer] conversationId=<id> answer="Weather: The weather in Tel Aviv is 28°C and Sunny."
    [Benchmark] conversationId=<id> synthesizerLatency=Xms

UI output:
    bot: Weather: The weather in Tel Aviv is 28°C and Sunny.

-------------------------------------------------------------------------------
Scenario 2 — Single-step: Currency Conversion
-------------------------------------------------------------------------------

User input
----------
    convert 100 usd to ils

Expected plan
-------------
    { "steps": ["exchange"] }

Event flow
----------
    UserQueryReceived       →  user-commands
    PlanGenerated           →  conversation-events
    ToolInvocationRequested →  tool-invocation-requests
    ToolInvocationResulted  →  conversation-events
    PlanCompleted           →  conversation-events
    FinalAnswerSynthesized  →  conversation-events  →  UI

Expected log sequence
---------------------

router.log:
    [router] conversationId=<id> plan=[exchange] input="convert 100 usd to ils"
    [Benchmark] conversationId=<id> routerLatency=Xms

orchestrator.log:
    [orchestrator] conversationId=<id> plan received steps=[exchange]
    [orchestrator] conversationId=<id> dispatching tool="exchange"
    [orchestrator] conversationId=<id> step 1/1 completed tool="exchange"
    [orchestrator] conversationId=<id> plan completed, results=1
    [Benchmark] conversationId=<id> workerLatency=Xms

exchange.log:
    [exchange] tool conversationId=<id> from=USD to=ILS amount=1
    [exchange] tool result="1 USD = 3.70 ILS"

answer.log:
    [synthesizer] conversationId=<id> results=1
    [synthesizer] conversationId=<id> answer="Exchange: 1 USD = 3.70 ILS"
    [Benchmark] conversationId=<id> synthesizerLatency=Xms

UI output:
    bot: Exchange: 1 USD = 3.70 ILS

-------------------------------------------------------------------------------
Scenario 3 — Multi-step Orchestration: Weather + Exchange
-------------------------------------------------------------------------------

User input
----------
    weather in tel aviv and convert usd to ils

Expected plan
-------------
    { "steps": ["weather", "exchange"] }

Event flow
----------
    UserQueryReceived           →  user-commands
    PlanGenerated               →  conversation-events
    ToolInvocationRequested     →  tool-invocation-requests   (weather)
    ToolInvocationResulted      →  conversation-events        (weather done)
    ToolInvocationRequested     →  tool-invocation-requests   (exchange)
    ToolInvocationResulted      →  conversation-events        (exchange done)
    PlanCompleted               →  conversation-events
    FinalAnswerSynthesized      →  conversation-events  →  UI

Note: the orchestrator executes steps sequentially. The exchange tool is only
dispatched after the weather result is received.

Expected log sequence
---------------------

router.log:
    [router] conversationId=<id> plan=[weather, exchange] input="weather in tel aviv and convert usd to ils"
    [Benchmark] conversationId=<id> routerLatency=Xms

orchestrator.log:
    [orchestrator] conversationId=<id> plan received steps=[weather, exchange]
    [orchestrator] conversationId=<id> dispatching tool="weather"
    [orchestrator] conversationId=<id> step 1/2 completed tool="weather"
    [orchestrator] conversationId=<id> dispatching tool="exchange"
    [orchestrator] conversationId=<id> step 2/2 completed tool="exchange"
    [orchestrator] conversationId=<id> plan completed, results=2
    [Benchmark] conversationId=<id> workerLatency=Xms

weather.log:
    [weather] tool conversationId=<id> city="tel aviv"
    [weather] tool result="The weather in Tel Aviv is 28°C and Sunny."

exchange.log:
    [exchange] tool conversationId=<id> from=USD to=ILS amount=1
    [exchange] tool result="1 USD = 3.70 ILS"

answer.log:
    [synthesizer] conversationId=<id> results=2
    [synthesizer] conversationId=<id> answer="Weather: The weather in Tel Aviv is 28°C and Sunny.\nExchange: 1 USD = 3.70 ILS"
    [Benchmark] conversationId=<id> synthesizerLatency=Xms

UI output:
    bot: Weather: The weather in Tel Aviv is 28°C and Sunny.
         Exchange: 1 USD = 3.70 ILS

-------------------------------------------------------------------------------
Running All Three Scenarios Back-to-Back
-------------------------------------------------------------------------------

With all services running, type the following inputs in sequence in the UI:

    weather in tel aviv
    convert 100 usd to ils
    weather in tel aviv and convert usd to ils

Each query produces a complete [Benchmark] line in all three service logs,
making it easy to compare single-step vs multi-step latency side by side.

To collect the benchmark lines from a run:

    grep "\[Benchmark\]" scripts/logs/final-project-services/*.log
