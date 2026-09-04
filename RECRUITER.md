# What this repository is

A trading system keeps its own record of every position it decided to take. The exchange keeps a
separate record of which of those positions actually settled and for how much. The two records
are written by different systems and drift apart, so a position the trading system thinks it
holds can go unreported by the exchange, and the money goes missing quietly. This project
reconciles the two. It takes 281 decisions from a six-day test of a trading rule on the Kalshi
exchange and matches them against the 263 rows the exchange settlement feed produced. The 18
that do not match are sorted into three groups: 6 the feed had not reached yet, 1 the exchange
closed with no result to score, and 11 with no explanation anywhere in the data. The unusual
part is what the project does with its own checks. A check that never fires looks exactly like a
clean set of books, so the project breaks the data on purpose fifteen times, once per check, and
confirms that each check catches its own fault.
