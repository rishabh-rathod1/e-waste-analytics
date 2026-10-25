# Data Dictionary & Material Flow Taxonomy

**Dataset**: FutuRaM European WEEE Stock and Flow Database (`WEEE_SF_cmp_version2.csv`)
**Size**: 2.22 GB (2,225,829,198 bytes), 17,521,915 records

---

## 1. Primary Dataset Schema

| Column Name | Raw Type | Target Type | Description & Domain Values |
|---|---|---|---|
| `Waste Stream` | String | String | Identifies waste domain: `"WEEE"` (Waste Electrical and Electronic Equipment) |
| `Location` | String | String | ISO-3166-1 alpha-3 European Country Code (31 countries, e.g. `DEU`, `FRA`, `GBR`, `ITA`, `ESP`) |
| `Year` | Integer | Integer | Temporal dimension spanning 2010 through 2050 |
| `Scenario` | String | String | Model trajectory: `OBS` (Observed historical 2010-2022), `BAU` (Business-As-Usual), `REC` (Recycling), `CIR` (Circular Economy) |
| `additionalSpecification` | String | String | Optional fine-grained specification (mostly null in stock/flow CSV) |
| `Stock/Flow ID` | String | String | Material flow classification identifier (see Section 2) |
| `Layer 1` | String | String | Primary EU WEEE Category (e.g., `WEEE_Cat1` to `WEEE_Cat6`) |
| `Layer 2` | String | String | Component/sub-assembly layer (e.g. `PCB`, `Cables`, `Batteries`). Blank for Category Summary. |
| `Layer 3` | String | String | Material group classification (e.g. `WEEEMaterialRest`, `Plastics`) |
| `Layer 4` | String | String | Elemental/chemical composition (e.g. `Cu`, `Al`, `Fe`, `Ag`, `Au`) |
| `Value` | Float/Double | Double | Quantitative physical mass in megagrams (metric tons) |
| `Unit` | String | String | Measurement unit: `"Mg"` (Megagrams = $10^6$ grams = 1 Metric Ton) |

---

## 2. Stock and Flow Taxonomy

| Stock/Flow ID | Flow Class | Description | Total Processed Mass (Mg) |
|---|---|---|---|
| `WEEE_EEE_stock` | Active Stock | Electrical and electronic equipment in active use by consumers and enterprises | 23,372,669,795.99 |
| `WEEE_EEEPOM` | Input Flow | Electrical and Electronic Equipment Put On Market (annual new sales) | 2,003,364,000.75 |
| `WEEE_generatedDedicated` | Primary Waste | E-waste entering dedicated, formal recycling and disposal channels | 1,358,123,388.21 |
| `WEEE_collected` | Recovery Flow | Formally reported e-waste collected under compliance systems | 935,320,432.75 |
| `WEEE_generatedComplementaryMetalScrap` | Informal Flow | E-waste routed into mixed scrap metal shredding | 265,451,452.89 |
| `WEEE_generatedComplementaryUndocumented` | Unrecorded Flow | E-waste traded or processed outside standard regulatory reporting | 75,391,547.57 |
| `WEEE_generatedComplementaryMSW` | Landfill/MSW | E-waste mistakenly discarded into municipal mixed municipal waste bins | 52,888,891.68 |
| `WEEE_generatedComplementaryExported` | Cross-border | Used and discarded equipment exported across international borders | 29,088,412.14 |

---

## 3. EU WEEE Category Mappings (Layer 1)

| Code | Standard EU Directive Designation | Examples | Total Generated (Mg) | Share (%) |
|---|---|---|---|---|
| `WEEE_Cat4a` | Large Equipment | Washing machines, dryers, dishwashers, electric stoves, large fans | 686,220,604.07 | 38.53% |
| `WEEE_Cat5` | Small Equipment | Vacuum cleaners, microwaves, toasters, power tools, electric shavers | 491,105,034.36 | 27.58% |
| `WEEE_Cat1` | Temperature Exchange | Refrigerators, freezers, air conditioning units, heat pumps | 285,626,337.29 | 16.04% |
| `WEEE_Cat6` | Small IT & Telecommunications | Smartphones, laptops, printers, routers, tablets, desktop PCs | 115,870,159.72 | 6.51% |
| `WEEE_Cat4b` | Photovoltaic Panels | Monocrystalline, polycrystalline, and thin-film solar PV panels | 97,670,400.11 | 5.48% |
| `WEEE_Cat2` | Screens and Monitors | Televisions, LCD/OLED monitors, digital displays | 90,290,709.29 | 5.07% |
| `WEEE_Cat3` | Lamps & Lighting | Fluorescent tubes, compact fluorescent lamps, high-intensity discharge, LED | 14,160,447.67 | 0.80% |

---

## 4. Geographic Country Mapping (31 Locations)
`AUT` (Austria), `BEL` (Belgium), `BGR` (Bulgaria), `CHE` (Switzerland), `CYP` (Cyprus), `CZE` (Czechia), `DEU` (Germany), `DNK` (Denmark), `ESP` (Spain), `EST` (Estonia), `FIN` (Finland), `FRA` (France), `GBR` (United Kingdom), `GRC` (Greece), `HRV` (Croatia), `HUN` (Hungary), `IRL` (Ireland), `ISL` (Iceland), `ITA` (Italy), `LTU` (Lithuania), `LUX` (Luxembourg), `LVA` (Latvia), `MLT` (Malta), `NLD` (Netherlands), `NOR` (Norway), `POL` (Poland), `PRT` (Portugal), `ROU` (Romania), `SVK` (Slovakia), `SVN` (Slovenia), `SWE` (Sweden).
