# Bayse Market Making Engine

A live-deployed algorithmic market making engine for binary prediction 
market contracts on Bayse Markets. Built in Python with a 
Binary Black-Scholes pricing core.

## What This Does

Prediction market contracts pay $1 on YES resolution and $0 on NO — 
structurally identical to cash-or-nothing digital options. This engine 
continuously quotes bid and ask prices around a theoretically grounded 
fair value, allowing other traders to buy or sell contracts against it 
at any time.

## Pricing Model

Fair value is computed using Binary Black-Scholes:
Fair Value = e^(-rT) × N(d2)
Where N(d2) is the risk-neutral probability of YES resolution. This is 
estimated using:
- Current market price as the underlying
- Rolling realised volatility via O(1) deque-based window update
- Time to market resolution as T

This gives a theoretically grounded fair value with proper Greeks — 
rather than simply quoting at the last traded price.

## Architecture

Three concurrent threads:

| Thread | Responsibility |
|--------|---------------|
| Feed thread | Consumes WebSocket price updates, computes rolling vol |
| Pricing thread | Recalculates N(d2) fair value, generates bid/ask quotes |
| Execution thread | Submits REST orders with HMAC-SHA256 authentication |

Threading is critical — a slow REST call must never block the next 
price update.

## Risk Controls

Three layered controls protect against adverse outcomes:

- **Inventory limit** — engine pauses quoting if net position exceeds 
  threshold, preventing unintended directional exposure
- **Drawdown pause** — session P&L monitored continuously; breach of 
  10% drawdown triggers automatic pause for manual review
- **Adverse selection detection** — fill-side asymmetry monitored over 
  a rolling window; above 70% one-sided fills triggers spread widening 
  or full pause
## Live Performance

Live trading session results from Bayse Markets platform:

| Session | Starting Wallet | Ending Wallet | P&L |
|---------|----------------|---------------|-----|
| SOL/USD Apr 13 | $49.42 | $56.80 | +$7.38 |
| SOL/USD Apr 13 | $51.78 | $54.89 | +$3.11 |
| SOL/USD Apr 12 | $44.36 | $56.77 | +$12.41 |

**Adverse selection detection confirmed in live conditions:**
Fill asymmetry at 67% UP-dominant triggered automatic spread 
widening and bid pause — engine responded correctly without 
manual intervention.

**Markets traded:** SOL/USD and EUR/GBP prediction pairs  
**Resolution window:** 15-minute contracts  
**Engine versions deployed:** v3, v4 (SOL MM)
  

## Project Structure
bayse_mm/
├── crypto_bot.py        # Core engine — pricing, threading, execution
├── test_crypto_bot.py   # Unit tests with correct argument signatures
└── .gitignore
## Testing

Unit tests cover core pricing logic and argument signatures:

```bash
python -m pytest test_crypto_bot.py -v
```

## Technical Stack

- **Language** — Python
- **Pricing model** — Binary Black-Scholes, N(d2) fair value
- **Concurrency** — Python threading module
- **Authentication** — HMAC-SHA256 REST order signing
- **Data structures** — deque-based O(1) rolling volatility

## Planned Improvements

Three upgrades on the roadmap:

1. **Heston stochastic volatility** — replace flat vol assumption with 
   mean-reverting vol process for better smile capture
2. **Brier score tracking** — measure calibration of N(d2) probability 
   estimates against actual resolution rates
3. **C++ pricing engine** — move BSM computation to C++ extension via 
   pybind11 for sub-millisecond latency

## Background

Built and deployed during a live quantitative analyst interview process 
on the Bayse Markets platform. Pricing model derived from first 
principles including an original Maclaurin series derivation of the 
GBM solution — available on 
[Substack](https://substack.com/@ayomideakinola1?utm_source=share&utm_medium=android&r=5xlptl).

## Author

**Ayomide Akinola Babajide**  
Quantitative Developer | Algorithmic Options Pricing & Market Making  
[LinkedIn](www.linkedin.com/in/ayomide-a-62b28928b) • [GitHub](https://github.com/ay007008)























