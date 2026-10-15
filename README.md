# Polymarket trading agent

A Polymarket trading system made of various components: An ETL pipeline that extracts Polymarket’s markets, processes and stores relevant data; selection of worthy markets to process and embellish with features for analysis; a websocket listening to those markets' events, and evaluating strategy and aggregated data with for actionable actions. Each stage has its own performance report, so a miss can be checked where it happened. Order placement is the next piece. The market rules were learned while the system was built, and they are still being tuned.

## ETL pipeline

A copy of the open markets' relevant data, on its own schedule, so a slow scan does not block the live path. Blockchain logs and subgraph queries were explored first. Signs that the subgraph data was unreliable led to this beta using Polymarket’s APIs instead, with a wider set of sources left for later. Fetching, processing, and saving run as separate concurrent work, and the result lands in Postgres.

## Selection

The system cannot afford a deep read of every market. On a faster schedule than the catalog, this stage ranks the catalog with a weighted score and ensures the appropriate data's available for the selected top. The weights are based off positive/negative market factors such as volume, open interest, and related-market structures to provide a balanced score. The published output is a board of N markets put on a watchlist. This is feature construction for that score. It is not a trained model.

## Live listener

A websocket watches and registers significant changes to the watchlist of worthy markets and has access to their aggregated data along with strategy logic in order to decide if and when an action should be taken in order to maximize portfolio performance.

## Evaluation

Reports are produced per stage, in order: the data, then the watchlist, then the trading book. These figures are a preliminary beta reading. The rules are still being tuned, and strategies and portfolio management logic's adjusted accordingly.

### Trading book, 7–21 August 2026

Latest frozen report. Starting capital $10,000. Every trade in the window is closed.

|                  |            |
| ---------------- | ---------- |
| Starting equity  | $10,000.00 |
| Ending equity    | $9,935.64  |
| Net P&L          | −$64.36    |
| Return           | −0.64%     |
| Trades closed    | 285        |
| Winners / losers | 65 / 220   |
| Hit rate         | 22.8%      |
| Average trade    | −$0.23     |
| Max drawdown     | 13.6%      |
| Worst day        | −3.4%      |

254,800 names appeared on boards. 50,960 reached the trading check. 285 were taken. Of the rest, 46,940 were refused because the drawdown stop had already fired, 3,525 because the market was already on, and 210 because the book was at its name cap. The loss is real and small. Most of the window was not traded, because the stop had halted new trades. That is a reason the book is still being tuned, not a hidden result.

## Why it is built this way

This was meant as a prototype for an autonomous multi-agent system, requiring accuracy and performance that could be verified and observed, and could be replicated in applications in other domains.

The stages are programs, not instructions to a model. With a multi-agent layout in mind, each pipeline is program an orchestrator’s sub-agent can run, so that each stage’s context stays separate and the orchestrator keeps a smaller, more focused context. A prompt can be written well and current models are very capable. However, the assumption is that putting the stage logic in code, and leaving the model to interpret the results, costs less context than asking the model to perform the logic and would lead to more optimal outputs. That assumption still has to be checked with a measurement.

The components run independently (though some stages do depend on previous stages' outputs to output meaningfully), with their own parameters, so the same skeleton can serve more than one use. That was deliberate room for a later system that reads other venues and other assets such as stocks.

The original aim was to analyze ingested data and derive specific insight from it. Blockchain logs and subgraph data were the first route for accuracy and flexibility. However, for simplicity and indications that the subgraph data could be unreliable are why this beta uses Polymarket’s API, with a broader set of sources to be added later.

Karpathy’s auto-researcher looked like the right tool for fast iteration on a prototype like this: try a concept, measure it, keep or drop it. The groundwork for it has the parametrization of the pieces that drive the system. That work is in progress.

The financial logic is not finished. I'll have to admit I somewhat underestimated building the system with a new multi-agent workflow while delving to the depths of Polymarket and finance. However, the engineering is in good shape for a beta. Jobs run concurrently, the runs are logged, the stages have tests, and each stage writes a report with verifiable metrics. A change to the finance rules is then a measured change, which I plan on being able to further abstract and automate with an auto-optimizer workflow I have in progress. The goal was a measurable, accurate, and stable agentic system that I could iterate upon the financial logic.

## Where it stands

The bottleneck is the finance rules, not the absence of a pipeline. Some of the stages along the system's logic such as market selection might need some adjustments, and now that I have a stable system with tests and metrics, verifiable improvements can be made. The next objective is the auto-optimizer: an automated research loop that proposes a change, runs the stage reports, and keeps or drops the change. Beside that, benchmarking the pipelines for verifying performance and tuning.
