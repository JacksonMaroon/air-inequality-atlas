# Glossary

**AQS (EPA Air Quality System)**: EPA system of record for air monitoring data; includes annual summaries. Not real-time (this is historical monitoring data).

**PLACES (CDC)**: Model-based local estimates (MRP) of health outcomes and risk factors, derived from BRFSS and other inputs. Treat as snapshot estimates, not county trend series.

**SVI (CDC/ATSDR Social Vulnerability Index)**: Relative percentile rankings of vulnerability (0-1). Higher means more vulnerable relative to other counties. Not directly comparable across years.

**CBI (Compound Burden Index)**: A composite "burden" score combining standardized pollution, health burden, and vulnerability.

## Metrics (Plain English)

**PM2.5 (annual mean)**: The county's annual average fine particulate concentration (ug/m3) from EPA AQS monitors (aggregated to county-year). Many counties have no monitors and show NA.

**Ozone (annual mean)**: The county's annual average ozone concentration (ppb) from EPA AQS monitors (aggregated to county-year). Many counties have no monitors and show NA.

**Asthma (PLACES age-adjusted %)**: CDC PLACES modeled asthma prevalence (percent) for a single snapshot year.

**COPD (PLACES age-adjusted %)**: CDC PLACES modeled COPD prevalence (percent) for a single snapshot year.

**SVI overall (percentile)**: Overall social vulnerability percentile (0-1) from CDC/ATSDR SVI.

**CBI (z-score)**: A weighted combination of standardized components:

- 40% PM2.5 (anchor year)
- 20% Ozone (anchor year)
- 20% Asthma (PLACES)
- 20% SVI (SVI year)

Counties only get a CBI score if all four components are available.

## Guardrails

- Not real-time: AQS is not live air quality.
- Not causal: maps and charts are descriptive (associations only).
- PLACES is modeled: not intended for county trend inference.
