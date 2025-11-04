# Pacotes
from google.cloud import bigquery
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
import pandas_gbq
import time
import numpy as np
from datetime import datetime, timedelta, date
from statsmodels.stats.power import NormalIndPower
from sklearn.model_selection import train_test_split
from math import ceil
from scipy import stats

client = bigquery.Client(project='credit-business-9fd5bf2eed53')

%%bigquery bq_testelast
SELECT  * FROM  `dataplatform-prd.credit_negocios.20251016_testelasticidade_pool`

df = bq_testelast.copy()

# ---------------------------
# 1) Randomização
# ---------------------------
def assign_treatment(
    df: pd.DataFrame,
    treat_share: float = 0.70,
    stratify_col: str | None = "GrupoKGiro",
    seed: int = 42,
    arm_col: str = "arm",
) -> pd.DataFrame:
    """
    Atribui tratamento/controle com proporção treat_share (ex.: 0.70).
    Por padrão, estratifica por 'GrupoKGiro' para garantir melhor balanceamento dessa categórica.
    """
    rng = np.random.default_rng(seed)
    out = df.copy()

    if stratify_col is None:
        # sem estratificação
        n = len(out)
        n_treat = int(round(treat_share * n))
        idx = np.arange(n)
        rng.shuffle(idx)
        treat_idx = set(idx[:n_treat])
        out[arm_col] = np.where(out.reset_index(drop=True).index.isin(treat_idx), "T", "C")
    else:
        # estratificado por categórica
        out[arm_col] = "C"
        for level, block in out.groupby(stratify_col, dropna=False):
            idx_block = block.index.to_numpy()
            n_block = len(idx_block)
            n_treat_block = int(round(treat_share * n_block))
            rng.shuffle(idx_block)
            treat_idx = idx_block[:n_treat_block]
            out.loc[treat_idx, arm_col] = "T"

    # checagem rápida da divisão
    share_t = (out[arm_col] == "T").mean()
    print(f"Proporção final no tratamento: {share_t:.3%} (target {treat_share:.0%})")
    return out


# ---------------------------
# 2) Métricas de balanceamento
# ---------------------------
def smd_numeric(x_t: pd.Series, x_c: pd.Series) -> float:
    """Standardized Mean Difference para variável numérica."""
    m_t, m_c = x_t.mean(), x_c.mean()
    s_t, s_c = x_t.std(ddof=1), x_c.std(ddof=1)
    # pooled SD (Hedges)
    s_pooled = np.sqrt(((len(x_t)-1)*s_t**2 + (len(x_c)-1)*s_c**2) / (len(x_t)+len(x_c)-2))
    return (m_t - m_c) / s_pooled if s_pooled > 0 else np.nan

def smd_prop(p_t: float, p_c: float) -> float:
    """SMD para proporções (por categoria): (p_t - p_c) / sqrt(p*(1-p))."""
    p = (p_t + p_c) / 2
    denom = np.sqrt(p * (1 - p))
    return (p_t - p_c) / denom if denom > 0 else np.nan

def balance_numeric(df: pd.DataFrame, var: str, arm_col: str = "arm") -> pd.DataFrame:
    x_t = df.loc[df[arm_col] == "T", var].dropna()
    x_c = df.loc[df[arm_col] == "C", var].dropna()

    # teste t com variâncias desiguais (Welch)
    t_stat, p_val = stats.ttest_ind(x_t, x_c, equal_var=False, nan_policy='omit')

    res = pd.DataFrame({
        "group": ["Treatment", "Control", "Diff (T - C)"],
        "n": [x_t.shape[0], x_c.shape[0], ""],
        "mean": [x_t.mean(), x_c.mean(), x_t.mean() - x_c.mean()],
        "std": [x_t.std(ddof=1), x_c.std(ddof=1), ""],
    })
    smd = smd_numeric(x_t, x_c)
    print(f"[{var}] SMD = {smd:.5f} | t-test p-value = {p_val:.5f}")
    return res

def balance_categorical(df: pd.DataFrame, var: str, arm_col: str = "arm") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna:
      - tabela de contingência com contagens e proporções,
      - tabela por categoria com diferença de proporções e SMD de proporções.
    Também imprime teste qui-quadrado e Cramér's V.
    """
    tab = pd.crosstab(df[arm_col], df[var], dropna=False)
    prop = tab.div(tab.sum(axis=0), axis=1)  # proporção por coluna (dentro da categoria)

    # teste qui-quadrado
    chi2, p_val, dof, exp = stats.chi2_contingency(tab)
    n = tab.values.sum()
    # Cramér's V
    k = min(tab.shape) - 1
    cramers_v = np.sqrt(chi2 / (n * k)) if k > 0 and n > 0 else np.nan
    print(f"[{var}] chi2 p-value = {p_val:.5f} | Cramér's V = {cramers_v:.5f}")

    # diffs e SMD por categoria (usando proporção em T e C)
    rows = []
    for cat in tab.columns:
        ct = tab.loc["T", cat] if "T" in tab.index else 0
        cc = tab.loc["C", cat] if "C" in tab.index else 0
        nt = tab.loc["T"].sum() if "T" in tab.index else 0
        nc = tab.loc["C"].sum() if "C" in tab.index else 0
        p_t = ct / nt if nt > 0 else np.nan
        p_c = cc / nc if nc > 0 else np.nan
        rows.append({
            var: cat,
            "p_T": p_t,
            "p_C": p_c,
            "Diff (T - C)": p_t - p_c if pd.notna(p_t) and pd.notna(p_c) else np.nan,
            "SMD_prop": smd_prop(p_t, p_c) if pd.notna(p_t) and pd.notna(p_c) else np.nan
        })
    diffs = pd.DataFrame(rows)

    # juntar contagens e proporções em uma tabela longa amigável
    tab_long = tab.T.copy()
    tab_long["Total"] = tab_long.sum(axis=1)
    prop_long = prop.T.copy()
    prop_long.columns = [f"{c}_prop" for c in prop_long.columns]
    summary = tab_long.join(prop_long)

    return summary.reset_index().rename(columns={"index": var}), diffs.sort_values("SMD_prop", key=lambda s: s.abs(), ascending=False)


# ---------------------------
# 3) Função principal de checagem
# ---------------------------
def check_balance(
    df: pd.DataFrame,
    numeric_var: str = "ExpectedRate",
    cat_var: str = "GrupoKGiro",
    arm_col: str = "arm",
) -> dict:
    out = {}

    print("\n=== Balanceamento: variável numérica ===")
    out["numeric_table"] = balance_numeric(df, numeric_var, arm_col)

    print("\n=== Balanceamento: variável categórica ===")
    cat_summary, cat_diffs = balance_categorical(df, cat_var, arm_col)
    out["categorical_counts_props"] = cat_summary
    out["categorical_diffs_smd"] = cat_diffs

    # Regras rápidas (heurísticas) de alerta visual
    smd_num = smd_numeric(
        df.loc[df[arm_col] == "T", numeric_var].dropna(),
        df.loc[df[arm_col] == "C", numeric_var].dropna()
    )
    flags = []
    if abs(smd_num) > 0.1:
        flags.append(f"SMD numérico {numeric_var} = {smd_num:.3f} (> 0.10)")

    worst_cat = cat_diffs["SMD_prop"].abs().max()
    if pd.notna(worst_cat) and worst_cat > 0.1:
        flags.append(f"Maior SMD de proporção em {cat_var} = {worst_cat:.3f} (> 0.10)")

    out["alerts"] = flags
    if flags:
        print("\n[ALERTAS] Possível desequilíbrio:\n  - " + "\n  - ".join(flags))
    else:
        print("\n[OK] SMDs dentro de |0.10| (regra prática comum).")

    return out


df_rct = assign_treatment(df, treat_share=0.70, stratify_col="GrupoKGiro", seed=2025, arm_col="arm")

results = check_balance(df_rct, numeric_var="ExpectedRate", cat_var="GrupoKGiro", arm_col="arm")
print(results["numeric_table"].to_string(index=False))
print(results["categorical_counts_props"].to_string(index=False))
print(results["categorical_diffs_smd"].to_string(index=False))


def balance_multiple_numerics(
    df: pd.DataFrame,
    numeric_vars: list[str],
    arm_col: str = "arm",
    smd_threshold: float = 0.10
) -> dict:
    """
    Para cada variável numérica em numeric_vars:
      - calcula n, média, desvio por Treatment/Control
      - Diff (T - C), SMD e p-valor (Welch t-test)
    Retorna:
      - 'by_var': dict {var: DataFrame detalhado por braço}
      - 'overview': DataFrame com SMD e p-valor por variável
      - 'alerts': lista de flags com |SMD| > smd_threshold
    """
    by_var = {}
    rows = []

    for var in numeric_vars:
        x_t = df.loc[df[arm_col] == "T", var].astype(float)
        x_c = df.loc[df[arm_col] == "C", var].astype(float)

        # estatísticas básicas
        m_t, m_c = x_t.mean(), x_c.mean()
        s_t, s_c = x_t.std(ddof=1), x_c.std(ddof=1)
        n_t, n_c = x_t.notna().sum(), x_c.notna().sum()

        # Welch t-test
        t_stat, p_val = stats.ttest_ind(x_t.dropna(), x_c.dropna(), equal_var=False)

        # SMD (pooled SD clássico)
        s_pooled = np.sqrt(((n_t-1)*(s_t**2) + (n_c-1)*(s_c**2)) / (n_t + n_c - 2)) if (n_t + n_c - 2) > 0 else np.nan
        smd = (m_t - m_c) / s_pooled if (s_pooled is not None and s_pooled > 0) else np.nan

        detail = pd.DataFrame({
            "group": ["Treatment", "Control", "Diff (T - C)"],
            "n": [n_t, n_c, ""],
            "mean": [m_t, m_c, m_t - m_c],
            "std": [s_t, s_c, ""],
            "SMD": [smd, "", smd],
            "p_value (Welch t)": [p_val, "", p_val]
        })
        by_var[var] = detail

        rows.append({
            "variable": var,
            "n_T": n_t, "n_C": n_c,
            "mean_T": m_t, "mean_C": m_c,
            "diff_T_minus_C": m_t - m_c,
            "SMD": smd,
            "p_value_Welch_t": p_val
        })

    overview = pd.DataFrame(rows).sort_values("SMD", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)

    # flags rápidas
    alerts = []
    for _, r in overview.iterrows():
        if pd.notna(r["SMD"]) and abs(r["SMD"]) > smd_threshold:
            alerts.append(f"{r['variable']}: |SMD| = {r['SMD']:.3f} (> {smd_threshold:.2f})")

    return {"by_var": by_var, "overview": overview, "alerts": alerts}


numeric_extra = ["CreditLimit", "Maturidade_real"]
res2 = balance_multiple_numerics(
        df_rct,
        numeric_vars=numeric_extra,
        arm_col="arm",
        smd_threshold=0.10
    )

print(res2["overview"].to_string(index=False))

df_rct['ProcessingDate'] = datetime.now()
pandas_gbq.to_gbq(
    df_rct,
    'dataplatform-prd.credit_negocios.20251016_testelasticidade_rct',
    project_id = client.project,
    if_exists = 'replace'
)