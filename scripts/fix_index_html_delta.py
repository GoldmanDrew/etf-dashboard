#!/usr/bin/env python3
from pathlib import Path

p = Path(__file__).resolve().parents[1] / "index.html"
t = p.read_text(encoding="utf-8")
repls = [
    ("{ key: 'beta', label: 'Beta'", "{ key: 'delta', label: 'Delta'"),
    ("const [beta, setBeta] = useState('2')", "const [hedgeDelta, setHedgeDelta] = useState('2')"),
    ("return fn({ beta, sigmaAnnual", "return fn({ delta: hedgeDelta, sigmaAnnual"),
    ("}, [beta, sigmaAnnual", "}, [hedgeDelta, sigmaAnnual"),
    ("<label>Beta</label>", "<label>Delta</label>"),
    ("value={beta} onChange={e => setBeta", "value={hedgeDelta} onChange={e => setHedgeDelta"),
    ("result.beta", "result.delta"),
    ("|beta|", "|delta|"),
    ("|beta − 1|", "|delta − 1|"),
    ("Example: beta=2", "Example: delta=2"),
    ("r.beta_n_obs", "r.delta_n_obs"),
    ("r.beta?", "r.delta?"),
    ("r.beta)", "r.delta)"),
    ("r.beta,", "r.delta,"),
    ("r?.beta", "r?.delta"),
    ("Number(r.beta)", "Number(r.delta)"),
    ("beta: Number(r?.beta)", "delta: Number(r?.delta)"),
    ('className="cs-label">Beta</motion.div>', 'className="cs-label">Delta</motion.div>'),
    ('className="dlabel">Beta (n =', 'className="dlabel">Delta (n ='),
    ("exp(beta * underlying", "exp(delta * underlying"),
    ("const lev = Number(r?.beta);", "const lev = Number(r?.delta);"),
    ('hint: \'ls-algo hedge β:', "hint: 'ls-algo hedge δ:"),
]
for a, b in repls:
    t = t.replace(a, b)
p.write_text(t, encoding="utf-8")
print("updated", p)
