//! The documented answers first, then one test per decision the module docs
//! make. Every one was seen red under a deliberate break before it was trusted.
//!
//! Citations are to `binance/binance-spot-api-docs` at commit `0556f1a`.

use core::time::Duration;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use quant_core::fixed::{Notional, ParseFixedError, Px, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind};

use super::{
    fetch, parse, ExchangeInfoError, FetchError, RuleScope, UnmodelledRule, VenueInstrument,
    MIN_NOTIONAL, NOTIONAL,
};
use crate::rest::{SnapshotClient, SnapshotError};

/// `rest-api.md` lines 812-876: the documented answer to
/// `GET /api/v3/exchangeInfo`, with its `//` comments and the blank lines they
/// leave removed and nothing else changed. Both filter lists are empty, since
/// the page describes filters in comments; so on its own this pins the
/// identity fields and what an absent filter becomes, and nothing about
/// reading one.
const REST_ANSWER: &str = r#"{
    "timezone": "UTC",
    "serverTime": 1565246363776,
    "rateLimits": [
        {
        }
    ],
    "exchangeFilters": [
    ],
    "symbols": [
        {
            "symbol": "ETHBTC",
            "status": "TRADING",
            "baseAsset": "ETH",
            "baseAssetPrecision": 8,
            "quoteAsset": "BTC",
            "quotePrecision": 8,
            "quoteAssetPrecision": 8,
            "baseCommissionPrecision": 8,
            "quoteCommissionPrecision": 8,
            "orderTypes": [
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT",
                "TAKE_PROFIT_LIMIT"
            ],
            "icebergAllowed": true,
            "ocoAllowed": true,
            "otoAllowed": true,
            "opoAllowed": true,
            "quoteOrderQtyMarketAllowed": true,
            "allowTrailingStop": false,
            "cancelReplaceAllowed": false,
            "amendAllowed": false,
            "pegInstructionsAllowed": true,
            "isSpotTradingAllowed": true,
            "isMarginTradingAllowed": true,
            "filters": [
            ],
            "permissions": [],
            "permissionSets": [["SPOT", "MARGIN"]],
            "defaultSelfTradePreventionMode": "NONE",
            "allowedSelfTradePreventionModes": ["NONE"]
        }
    ],
    "sors": [
        {
            "baseAsset": "BTC",
            "symbols": ["BTCUSDT", "BTCUSDC"]
        }
    ]
}"#;

/// `web-socket-api.md` lines 1534-1628: the same answer from the WebSocket
/// API's `exchangeInfo`, its `result` object alone, comments removed as above.
/// The one documented answer whose symbol carries filters.
const WS_ANSWER: &str = r#"{
    "timezone": "UTC",
    "serverTime": 1655969291181,
    "rateLimits": [
        {
            "rateLimitType": "REQUEST_WEIGHT",
            "interval": "MINUTE",
            "intervalNum": 1,
            "limit": 6000
        },
        {
            "rateLimitType": "ORDERS",
            "interval": "SECOND",
            "intervalNum": 10,
            "limit": 50
        },
        {
            "rateLimitType": "ORDERS",
            "interval": "DAY",
            "intervalNum": 1,
            "limit": 160000
        },
        {
            "rateLimitType": "CONNECTIONS",
            "interval": "MINUTE",
            "intervalNum": 5,
            "limit": 300
        }
    ],
    "exchangeFilters": [],
    "symbols": [
        {
            "symbol": "BNBBTC",
            "status": "TRADING",
            "baseAsset": "BNB",
            "baseAssetPrecision": 8,
            "quoteAsset": "BTC",
            "quotePrecision": 8,
            "quoteAssetPrecision": 8,
            "baseCommissionPrecision": 8,
            "quoteCommissionPrecision": 8,
            "orderTypes": [
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT_LIMIT"
            ],
            "icebergAllowed": true,
            "ocoAllowed": true,
            "otoAllowed": true,
            "opoAllowed": true,
            "quoteOrderQtyMarketAllowed": true,
            "allowTrailingStop": true,
            "cancelReplaceAllowed": true,
            "amendAllowed": false,
            "pegInstructionsAllowed": true,
            "isSpotTradingAllowed": true,
            "isMarginTradingAllowed": true,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.00000100",
                    "maxPrice": "100000.00000000",
                    "tickSize": "0.00000100"
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00100000",
                    "maxQty": "100000.00000000",
                    "stepSize": "0.00100000"
                }
            ],
            "permissions": [],
            "permissionSets": [["SPOT", "MARGIN", "TRD_GRP_004"]],
            "defaultSelfTradePreventionMode": "NONE",
            "allowedSelfTradePreventionModes": ["NONE"]
        }
    ],
    "sors": [
        {
            "baseAsset": "BTC",
            "symbols": ["BTCUSDT", "BTCUSDC"]
        }
    ]
}"#;

/// Every symbol filter example in `filters.md`, in the page's order and as the
/// page writes them: lines 52-57, 73-78, 100-107, 126-131, 149-154, 173-180,
/// 188-191, 209-214, 223-226, 235-238, 247-250, 266-269, 292-298, 310-313 and
/// 323-326.
const SYMBOL_FILTER_EXAMPLES: &str = r#"[
{
    "filterType": "PRICE_FILTER",
    "minPrice": "0.00000100",
    "maxPrice": "100000.00000000",
    "tickSize": "0.00000100"
},
{
    "filterType": "PERCENT_PRICE",
    "multiplierUp": "1.3000",
    "multiplierDown": "0.7000",
    "avgPriceMins": 5
},
{
    "filterType": "PERCENT_PRICE_BY_SIDE",
    "bidMultiplierUp": "1.2",
    "bidMultiplierDown": "0.2",
    "askMultiplierUp": "5",
    "askMultiplierDown": "0.8",
    "avgPriceMins": 1
},
{
    "filterType": "LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
},
{
    "filterType": "MIN_NOTIONAL",
    "minNotional": "0.00100000",
    "applyToMarket": true,
    "avgPriceMins": 5
},
{
    "filterType": "NOTIONAL",
    "minNotional": "10.00000000",
    "applyMinToMarket": false,
    "maxNotional": "10000.00000000",
    "applyMaxToMarket": false,
    "avgPriceMins": 5
},
{
    "filterType": "ICEBERG_PARTS",
    "limit": 10
},
{
    "filterType": "MARKET_LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
},
{
    "filterType": "MAX_NUM_ORDERS",
    "maxNumOrders": 25
},
{
    "filterType": "MAX_NUM_ALGO_ORDERS",
    "maxNumAlgoOrders": 5
},
{
    "filterType": "MAX_NUM_ICEBERG_ORDERS",
    "maxNumIcebergOrders": 5
},
{
    "filterType": "MAX_POSITION",
    "maxPosition": "10.00000000"
},
{
    "filterType": "TRAILING_DELTA",
    "minTrailingAboveDelta": 10,
    "maxTrailingAboveDelta": 2000,
    "minTrailingBelowDelta": 10,
    "maxTrailingBelowDelta": 2000
},
{
    "filterType": "MAX_NUM_ORDER_AMENDS",
    "maxNumOrderAmends": 10
},
{
    "filterType": "MAX_NUM_ORDER_LISTS",
    "maxNumOrderLists": 20
}
]"#;

/// Every exchange filter example in `filters.md`: lines 337-340, 349-352,
/// 360-363 and 373-376. The page's last example, `MAX_ASSET` (lines 393-398),
/// is in the `/myFilters` format and cannot appear in this answer
/// (`rest-api.md` line 4910).
const EXCHANGE_FILTER_EXAMPLES: &str = r#"[
{
    "filterType": "EXCHANGE_MAX_NUM_ORDERS",
    "maxNumOrders": 1000
},
{
    "filterType": "EXCHANGE_MAX_NUM_ALGO_ORDERS",
    "maxNumAlgoOrders": 200
},
{
    "filterType": "EXCHANGE_MAX_NUM_ICEBERG_ORDERS",
    "maxNumIcebergOrders": 10000
},
{
    "filterType": "EXCHANGE_MAX_NUM_ORDER_LISTS",
    "maxNumOrderLists": 20
}
]"#;

fn px(text: &str) -> Px {
    text.parse().expect("a valid price")
}

fn qty(text: &str) -> Qty {
    text.parse().expect("a valid quantity")
}

fn notional(text: &str) -> Notional {
    text.parse().expect("a valid notional")
}

/// `text` with the first `from` replaced by `to`, insisting `from` is there. A
/// replacement that matched nothing would leave the fixture unchanged, and the
/// test would quietly check the documented answer instead of the edit.
fn edit(text: &str, from: &str, to: &str) -> String {
    assert!(
        text.contains(from),
        "the fixture no longer contains {from:?}"
    );
    text.replacen(from, to, 1)
}

/// The documented REST answer, with its symbol's empty filter list replaced by
/// `filters`, a JSON list.
fn with_filters(filters: &str) -> String {
    edit(
        REST_ANSWER,
        "\"filters\": [\n            ]",
        &format!("\"filters\": {filters}"),
    )
}

/// Read the documented REST answer's symbol, ETHBTC, out of `answer`.
fn ethbtc_from(answer: &str) -> Result<VenueInstrument, ExchangeInfoError> {
    parse("ETHBTC", answer.as_bytes())
}

/// ETHBTC as the documented REST answer names it, with the three rules given.
fn ethbtc(tick: &str, lot: &str, floor: &str) -> InstrumentDef {
    InstrumentDef {
        exchange: Exchange::Binance,
        symbol: "ETHBTC".to_owned(),
        base: "ETH".to_owned(),
        quote: "BTC".to_owned(),
        kind: InstrumentKind::Spot,
        tick_size: px(tick),
        lot_size: qty(lot),
        min_notional: notional(floor),
    }
}

fn reported(scope: RuleScope, name: &str, fields: &[(&str, &str)]) -> UnmodelledRule {
    UnmodelledRule {
        scope,
        name: name.to_owned(),
        fields: fields
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

/// A symbol rule the definition does not carry, fields in the venue's names'
/// sort order.
fn rule(name: &str, fields: &[(&str, &str)]) -> UnmodelledRule {
    reported(RuleScope::Symbol, name, fields)
}

fn exchange_rule(name: &str, fields: &[(&str, &str)]) -> UnmodelledRule {
    reported(RuleScope::Exchange, name, fields)
}

/// A rate limit the answer lists, as it is reported: whole, exchange-wide,
/// under the name of the list.
fn rate_limit(fields: &[(&str, &str)]) -> UnmodelledRule {
    exchange_rule("rateLimits", fields)
}

/// The documented REST answer's one rate limit. The page gives it as a
/// placeholder holding only comments (`rest-api.md` lines 816-821), so with
/// them removed it is an entry with no fields: reported as one, not dropped.
fn placeholder_rate_limit() -> UnmodelledRule {
    rate_limit(&[])
}

/// Everything an answer built on [`REST_ANSWER`] reports, given what its
/// filters report: those, then the documented answer's own rate limit.
fn ethbtc_reports(filter_rules: Vec<UnmodelledRule>) -> Vec<UnmodelledRule> {
    let mut all = filter_rules;
    all.push(placeholder_rate_limit());
    all
}

#[test]
fn the_documented_answer_parses_exactly() {
    // No filters at all, so no rules: every one of the three is zero, which
    // `Filters` reads as none. Not the journal's `MIN_TICK` placeholders and
    // not the literals every binary registers today -- either would be a rule
    // the venue did not state. All it reports is its one rate limit, which
    // the page leaves as a placeholder.
    assert_eq!(
        ethbtc_from(REST_ANSWER),
        Ok(VenueInstrument {
            definition: ethbtc("0", "0", "0"),
            unmodelled: vec![placeholder_rate_limit()],
        })
    );
}

#[test]
fn the_documented_answer_with_filters_parses_exactly() {
    let got = parse("BNBBTC", WS_ANSWER.as_bytes()).expect("the documented answer parses");
    assert_eq!(
        got.definition,
        InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BNBBTC".to_owned(),
            base: "BNB".to_owned(),
            quote: "BTC".to_owned(),
            kind: InstrumentKind::Spot,
            // Lines 1605 and 1611. No notional filter, so no floor.
            tick_size: px("0.00000100"),
            lot_size: qty("0.00100000"),
            min_notional: Notional::ZERO,
        }
    );
    // What the two filters say beyond the step: the bounds the model has no
    // field for, as the venue wrote them. Then the four rate limits (lines
    // 1538-1563), the two `ORDERS` limits among them, each whole.
    assert_eq!(
        got.unmodelled,
        vec![
            rule(
                "PRICE_FILTER",
                &[("maxPrice", "100000.00000000"), ("minPrice", "0.00000100")]
            ),
            rule(
                "LOT_SIZE",
                &[("maxQty", "100000.00000000"), ("minQty", "0.00100000")]
            ),
            rate_limit(&[
                ("interval", "MINUTE"),
                ("intervalNum", "1"),
                ("limit", "6000"),
                ("rateLimitType", "REQUEST_WEIGHT"),
            ]),
            rate_limit(&[
                ("interval", "SECOND"),
                ("intervalNum", "10"),
                ("limit", "50"),
                ("rateLimitType", "ORDERS"),
            ]),
            rate_limit(&[
                ("interval", "DAY"),
                ("intervalNum", "1"),
                ("limit", "160000"),
                ("rateLimitType", "ORDERS"),
            ]),
            rate_limit(&[
                ("interval", "MINUTE"),
                ("intervalNum", "5"),
                ("limit", "300"),
                ("rateLimitType", "CONNECTIONS"),
            ]),
        ]
    );
}

#[test]
fn every_filters_md_example_parses_exactly() {
    let answer = edit(
        &with_filters(SYMBOL_FILTER_EXAMPLES),
        "\"exchangeFilters\": [\n    ]",
        &format!("\"exchangeFilters\": {EXCHANGE_FILTER_EXAMPLES}"),
    );
    let got = ethbtc_from(&answer).expect("every documented filter parses");

    // The tick from line 56, the lot from line 130, and the floor from
    // `NOTIONAL` (line 175): ten, against `MIN_NOTIONAL`'s 0.001 (line 151).
    assert_eq!(
        got.definition,
        ethbtc("0.00000100", "0.00100000", "10.00000000")
    );

    // Everything else, nothing dropped and nothing interpreted: the whole list,
    // in the page's order, compared exactly.
    assert_eq!(
        got.unmodelled,
        ethbtc_reports(vec![
            rule(
                "PRICE_FILTER",
                &[("maxPrice", "100000.00000000"), ("minPrice", "0.00000100")]
            ),
            rule(
                "PERCENT_PRICE",
                &[
                    ("avgPriceMins", "5"),
                    ("multiplierDown", "0.7000"),
                    ("multiplierUp", "1.3000"),
                ]
            ),
            rule(
                "PERCENT_PRICE_BY_SIDE",
                &[
                    ("askMultiplierDown", "0.8"),
                    ("askMultiplierUp", "5"),
                    ("avgPriceMins", "1"),
                    ("bidMultiplierDown", "0.2"),
                    ("bidMultiplierUp", "1.2"),
                ]
            ),
            rule(
                "LOT_SIZE",
                &[("maxQty", "100000.00000000"), ("minQty", "0.00100000")]
            ),
            // The smaller floor, so the definition did not take it: reported
            // whole, `minNotional` included.
            rule(
                "MIN_NOTIONAL",
                &[
                    ("applyToMarket", "true"),
                    ("avgPriceMins", "5"),
                    ("minNotional", "0.00100000"),
                ]
            ),
            rule(
                "NOTIONAL",
                &[
                    ("applyMaxToMarket", "false"),
                    ("applyMinToMarket", "false"),
                    ("avgPriceMins", "5"),
                    ("maxNotional", "10000.00000000"),
                ]
            ),
            rule("ICEBERG_PARTS", &[("limit", "10")]),
            rule(
                "MARKET_LOT_SIZE",
                &[
                    ("maxQty", "100000.00000000"),
                    ("minQty", "0.00100000"),
                    ("stepSize", "0.00100000"),
                ]
            ),
            rule("MAX_NUM_ORDERS", &[("maxNumOrders", "25")]),
            rule("MAX_NUM_ALGO_ORDERS", &[("maxNumAlgoOrders", "5")]),
            rule("MAX_NUM_ICEBERG_ORDERS", &[("maxNumIcebergOrders", "5")]),
            rule("MAX_POSITION", &[("maxPosition", "10.00000000")]),
            rule(
                "TRAILING_DELTA",
                &[
                    ("maxTrailingAboveDelta", "2000"),
                    ("maxTrailingBelowDelta", "2000"),
                    ("minTrailingAboveDelta", "10"),
                    ("minTrailingBelowDelta", "10"),
                ]
            ),
            rule("MAX_NUM_ORDER_AMENDS", &[("maxNumOrderAmends", "10")]),
            rule("MAX_NUM_ORDER_LISTS", &[("maxNumOrderLists", "20")]),
            exchange_rule("EXCHANGE_MAX_NUM_ORDERS", &[("maxNumOrders", "1000")]),
            exchange_rule(
                "EXCHANGE_MAX_NUM_ALGO_ORDERS",
                &[("maxNumAlgoOrders", "200")]
            ),
            exchange_rule(
                "EXCHANGE_MAX_NUM_ICEBERG_ORDERS",
                &[("maxNumIcebergOrders", "10000")]
            ),
            exchange_rule(
                "EXCHANGE_MAX_NUM_ORDER_LISTS",
                &[("maxNumOrderLists", "20")]
            ),
        ])
    );
}

#[test]
fn each_rule_is_read_from_its_own_field() {
    // Every documented example gives `minPrice` the tick's value and `minQty`
    // the step's (`filters.md` lines 54 and 56, 128 and 130), so a parser
    // reading the wrong field of the right filter passes all of them. Here
    // every field differs from every other.
    let filters = r#"[
        {"filterType": "PRICE_FILTER", "minPrice": "0.02000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
        {"filterType": "LOT_SIZE", "minQty": "0.00020000", "maxQty": "9000.00000000", "stepSize": "0.00010000"},
        {"filterType": "NOTIONAL", "minNotional": "5.00000000", "applyMinToMarket": true, "maxNotional": "9000000.00000000", "applyMaxToMarket": false, "avgPriceMins": 5}
    ]"#;
    let got = ethbtc_from(&with_filters(filters)).expect("parses");
    assert_eq!(got.definition, ethbtc("0.01", "0.0001", "5"));
    assert_eq!(
        got.unmodelled,
        ethbtc_reports(vec![
            rule(
                "PRICE_FILTER",
                &[("maxPrice", "1000000.00000000"), ("minPrice", "0.02000000")]
            ),
            rule(
                "LOT_SIZE",
                &[("maxQty", "9000.00000000"), ("minQty", "0.00020000")]
            ),
            rule(
                "NOTIONAL",
                &[
                    ("applyMaxToMarket", "false"),
                    ("applyMinToMarket", "true"),
                    ("avgPriceMins", "5"),
                    ("maxNotional", "9000000.00000000"),
                ]
            ),
        ])
    );
}

#[test]
fn a_missing_or_empty_asset_is_an_error_naming_it() {
    // Empty names are exactly what make the user-data translator refuse at
    // startup, so an answer without them is refused here, by name, rather than
    // turned into a definition that fails later and elsewhere.
    for (field, as_listed) in [
        ("baseAsset", "\"baseAsset\": \"ETH\","),
        ("quoteAsset", "\"quoteAsset\": \"BTC\","),
    ] {
        for replacement in [
            String::new(),
            format!("\"{field}\": \"\","),
            format!("\"{field}\": null,"),
        ] {
            assert_eq!(
                ethbtc_from(&edit(REST_ANSWER, as_listed, &replacement)),
                Err(ExchangeInfoError::MissingField(field)),
                "{field} as {replacement:?}"
            );
        }
        // Not a string at all: also refused, also by name.
        assert_eq!(
            ethbtc_from(&edit(REST_ANSWER, as_listed, &format!("\"{field}\": 7,"))),
            Err(ExchangeInfoError::Malformed {
                field,
                value: "7".to_owned()
            })
        );
        // And the message a person reads at startup names it too.
        let said = ExchangeInfoError::MissingField(field).to_string();
        assert!(said.contains(field), "{said}");
    }
}

#[test]
fn market_lot_size_is_never_the_lot() {
    // `filters.md`'s own examples give both filters the same step (lines 130
    // and 213), so they cannot tell the two apart. These steps differ.
    let lot = r#"{"filterType": "LOT_SIZE", "minQty": "0.00100000", "maxQty": "9000.00000000", "stepSize": "0.00100000"}"#;
    let market = r#"{"filterType": "MARKET_LOT_SIZE", "minQty": "0.00000000", "maxQty": "250.00000000", "stepSize": "0.10000000"}"#;
    let market_rule = rule(
        "MARKET_LOT_SIZE",
        &[
            ("maxQty", "250.00000000"),
            ("minQty", "0.00000000"),
            ("stepSize", "0.10000000"),
        ],
    );

    // Whichever the venue lists first.
    for filters in [format!("[{lot}, {market}]"), format!("[{market}, {lot}]")] {
        let got = ethbtc_from(&with_filters(&filters)).expect("parses");
        assert_eq!(got.definition.lot_size, qty("0.001"), "{filters}");
        assert!(got.unmodelled.contains(&market_rule), "{filters}");
    }

    // And with no `LOT_SIZE` at all the lot is none, not the market order's
    // step borrowed to fill the gap.
    let got = ethbtc_from(&with_filters(&format!("[{market}]"))).expect("parses");
    assert_eq!(got.definition.lot_size, Qty::ZERO);
    assert_eq!(got.unmodelled, ethbtc_reports(vec![market_rule]));
}

#[test]
fn a_rule_the_model_cannot_express_is_reported_not_dropped() {
    // Three ways a rule could go missing: a filter type no page documents yet,
    // one with no fields at all -- its presence is the rule -- and a field
    // added to a filter the definition reads part of. These names are not the
    // venue's: they stand for what it may add.
    let filters = r#"[
        {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000", "futureBound": "7"},
        {"filterType": "FUTURE_RULE", "limit": "3", "applies": true},
        {"filterType": "BARE_RULE"}
    ]"#;
    let got = ethbtc_from(&with_filters(filters)).expect("parses");
    assert_eq!(got.definition.tick_size, px("0.01"));
    assert_eq!(
        got.unmodelled,
        ethbtc_reports(vec![
            rule(
                "PRICE_FILTER",
                &[
                    ("futureBound", "7"),
                    ("maxPrice", "1000000.00000000"),
                    ("minPrice", "0.01000000"),
                ]
            ),
            rule("FUTURE_RULE", &[("applies", "true"), ("limit", "3")]),
            rule("BARE_RULE", &[]),
        ])
    );

    // A filter the definition took all of is not reported: none of it is
    // missing from the model.
    let whole = r#"[{"filterType": "PRICE_FILTER", "tickSize": "0.01000000"}]"#;
    assert_eq!(
        ethbtc_from(&with_filters(whole))
            .expect("parses")
            .unmodelled,
        ethbtc_reports(Vec::new())
    );
}

#[test]
fn a_malformed_decimal_is_a_loud_error_naming_the_field() {
    use ExchangeInfoError::{BadNumber, Malformed, MissingField};
    let cases = [
        (
            r#"{"filterType": "PRICE_FILTER", "tickSize": "0.01x"}"#,
            BadNumber {
                field: "PRICE_FILTER.tickSize",
                value: "0.01x".to_owned(),
                source: ParseFixedError::InvalidChar('x'),
            },
        ),
        (
            r#"{"filterType": "LOT_SIZE", "stepSize": "0.000000001"}"#,
            BadNumber {
                field: "LOT_SIZE.stepSize",
                value: "0.000000001".to_owned(),
                source: ParseFixedError::PrecisionLoss { max_decimals: 8 },
            },
        ),
        (
            r#"{"filterType": "NOTIONAL", "minNotional": "1e1"}"#,
            BadNumber {
                field: "NOTIONAL.minNotional",
                value: "1e1".to_owned(),
                source: ParseFixedError::InvalidChar('e'),
            },
        ),
        (
            r#"{"filterType": "MIN_NOTIONAL", "minNotional": ""}"#,
            BadNumber {
                field: "MIN_NOTIONAL.minNotional",
                value: String::new(),
                source: ParseFixedError::Empty,
            },
        ),
        // A bare number: reading it would mean going through a float.
        (
            r#"{"filterType": "PRICE_FILTER", "tickSize": 0.01}"#,
            Malformed {
                field: "PRICE_FILTER.tickSize",
                value: "0.01".to_owned(),
            },
        ),
        (
            r#"{"filterType": "LOT_SIZE", "minQty": "0.00100000"}"#,
            MissingField("LOT_SIZE.stepSize"),
        ),
    ];
    for (filter, expected) in cases {
        assert_eq!(
            ethbtc_from(&with_filters(&format!("[{filter}]"))),
            Err(expected),
            "{filter}"
        );
    }

    let said = BadNumber {
        field: "PRICE_FILTER.tickSize",
        value: "0.01x".to_owned(),
        source: ParseFixedError::InvalidChar('x'),
    }
    .to_string();
    assert!(
        said.contains("PRICE_FILTER.tickSize") && said.contains("0.01x"),
        "{said}"
    );
}

#[test]
fn a_symbol_not_listed_is_its_own_error() {
    // Asked for one symbol, answered with another.
    assert_eq!(
        parse("BTCUSDT", REST_ANSWER.as_bytes()),
        Err(ExchangeInfoError::SymbolAbsent("BTCUSDT".to_owned()))
    );
    // Answered with none.
    assert_eq!(
        parse("ETHBTC", br#"{"exchangeFilters": [], "symbols": []}"#),
        Err(ExchangeInfoError::SymbolAbsent("ETHBTC".to_owned()))
    );
    // Listed twice, with different rules: which entry is the venue's cannot be
    // told, and taking either would be a guess.
    let twice = br#"{"exchangeFilters": [], "symbols": [
        {"symbol": "ETHBTC", "status": "TRADING", "baseAsset": "ETH", "quoteAsset": "BTC",
         "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.00000100"}]},
        {"symbol": "ETHBTC", "status": "TRADING", "baseAsset": "ETH", "quoteAsset": "BTC",
         "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.00001000"}]}
    ]}"#;
    assert_eq!(
        parse("ETHBTC", twice),
        Err(ExchangeInfoError::SymbolRepeated("ETHBTC".to_owned()))
    );
    // Not an answer at all.
    assert!(matches!(
        parse("ETHBTC", br#"{"code": -1121, "msg": "Invalid symbol."}"#),
        Err(ExchangeInfoError::NotExchangeInfo(_))
    ));
}

#[test]
fn a_symbol_not_trading_is_its_own_error() {
    // Every documented status but `TRADING` (`enums.md` lines 8-11).
    for status in ["END_OF_DAY", "HALT", "BREAK", "CANCEL_ONLY"] {
        assert_eq!(
            ethbtc_from(&edit(
                REST_ANSWER,
                "\"status\": \"TRADING\"",
                &format!("\"status\": \"{status}\"")
            )),
            Err(ExchangeInfoError::NotTrading {
                symbol: "ETHBTC".to_owned(),
                status: status.to_owned(),
            })
        );
    }
    // No status is not a status of `TRADING`.
    assert_eq!(
        ethbtc_from(&edit(REST_ANSWER, "\"status\": \"TRADING\",", "")),
        Err(ExchangeInfoError::MissingField("status"))
    );
}

#[test]
fn zero_disables_a_tick_and_a_floor_and_is_refused_as_a_lot_step() {
    // Both directions of one boundary. A zero tick is documented as switching
    // the rule off (`filters.md` line 42), and a zero floor is met by every
    // order, so both are read as no rule rather than refused...
    let disabled = r#"[
        {"filterType": "PRICE_FILTER", "minPrice": "0.00000000", "maxPrice": "0.00000000", "tickSize": "0.00000000"},
        {"filterType": "NOTIONAL", "minNotional": "0.00000000", "applyMinToMarket": true, "maxNotional": "9000000.00000000", "applyMaxToMarket": false, "avgPriceMins": 5}
    ]"#;
    let got = ethbtc_from(&with_filters(disabled)).expect("a disabled rule is not an error");
    assert_eq!(got.definition.tick_size, Px::ZERO);
    assert_eq!(got.definition.min_notional, Notional::ZERO);

    // ...while a zero lot step has no documented meaning, and is refused rather
    // than read as no lot rule.
    let zero_step = r#"[{"filterType": "LOT_SIZE", "minQty": "0.00100000", "maxQty": "9000.00000000", "stepSize": "0.00000000"}]"#;
    assert_eq!(
        ethbtc_from(&with_filters(zero_step)),
        Err(ExchangeInfoError::ZeroLotStep)
    );
}

#[test]
fn a_rule_below_zero_is_refused() {
    for (filter, field, value) in [
        (
            r#"{"filterType": "PRICE_FILTER", "tickSize": "-0.01"}"#,
            "PRICE_FILTER.tickSize",
            "-0.01",
        ),
        (
            r#"{"filterType": "LOT_SIZE", "stepSize": "-0.001"}"#,
            "LOT_SIZE.stepSize",
            "-0.001",
        ),
        (
            r#"{"filterType": "MIN_NOTIONAL", "minNotional": "-5"}"#,
            "MIN_NOTIONAL.minNotional",
            "-5",
        ),
        (
            r#"{"filterType": "NOTIONAL", "minNotional": "-5"}"#,
            "NOTIONAL.minNotional",
            "-5",
        ),
    ] {
        assert_eq!(
            ethbtc_from(&with_filters(&format!("[{filter}]"))),
            Err(ExchangeInfoError::Negative {
                field,
                value: value.to_owned()
            }),
            "{filter}"
        );
    }
}

#[test]
fn a_filter_the_definition_reads_cannot_be_listed_twice() {
    for (filter_type, a, b) in [
        (
            "PRICE_FILTER",
            r#"{"filterType": "PRICE_FILTER", "tickSize": "0.01"}"#,
            r#"{"filterType": "PRICE_FILTER", "tickSize": "0.1"}"#,
        ),
        (
            "LOT_SIZE",
            r#"{"filterType": "LOT_SIZE", "stepSize": "0.001"}"#,
            r#"{"filterType": "LOT_SIZE", "stepSize": "0.01"}"#,
        ),
        (
            MIN_NOTIONAL,
            r#"{"filterType": "MIN_NOTIONAL", "minNotional": "5"}"#,
            r#"{"filterType": "MIN_NOTIONAL", "minNotional": "10"}"#,
        ),
        (
            NOTIONAL,
            r#"{"filterType": "NOTIONAL", "minNotional": "5"}"#,
            r#"{"filterType": "NOTIONAL", "minNotional": "10"}"#,
        ),
    ] {
        assert_eq!(
            ethbtc_from(&with_filters(&format!("[{a}, {b}]"))),
            Err(ExchangeInfoError::RepeatedFilter(filter_type)),
            "{filter_type}"
        );
    }

    // A rule the definition does not read may repeat: both are reported, and
    // neither is chosen, since nothing here enforces either.
    let orders = r#"{"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 25}"#;
    let got = ethbtc_from(&with_filters(&format!("[{orders}, {orders}]"))).expect("parses");
    let once = rule("MAX_NUM_ORDERS", &[("maxNumOrders", "25")]);
    assert_eq!(got.unmodelled, ethbtc_reports(vec![once.clone(), once]));
}

/// A `MIN_NOTIONAL` filter with `floor`, as the venue would list it.
fn older_filter(floor: &str) -> String {
    format!(
        r#"{{"filterType": "MIN_NOTIONAL", "minNotional": "{floor}", "applyToMarket": true, "avgPriceMins": 5}}"#
    )
}

/// A `NOTIONAL` filter with `floor`, as the venue would list it.
fn newer_filter(floor: &str) -> String {
    format!(
        r#"{{"filterType": "NOTIONAL", "minNotional": "{floor}", "applyMinToMarket": false, "maxNotional": "9000000.00000000", "applyMaxToMarket": false, "avgPriceMins": 5}}"#
    )
}

/// What [`older_filter`] reports: everything, less `minNotional` if the
/// definition took it.
fn older_reported(floor: &str, taken: bool) -> UnmodelledRule {
    let mut fields = vec![("applyToMarket", "true"), ("avgPriceMins", "5")];
    if !taken {
        fields.push(("minNotional", floor));
    }
    rule(MIN_NOTIONAL, &fields)
}

/// What [`newer_filter`] reports: everything, less `minNotional` if the
/// definition took it.
fn newer_reported(floor: &str, taken: bool) -> UnmodelledRule {
    let mut fields = vec![
        ("applyMaxToMarket", "false"),
        ("applyMinToMarket", "false"),
        ("avgPriceMins", "5"),
        ("maxNotional", "9000000.00000000"),
    ];
    if !taken {
        fields.push(("minNotional", floor));
    }
    rule(NOTIONAL, &fields)
}

#[test]
fn the_larger_notional_floor_binds_and_the_other_is_reported_whole() {
    // Either filter can hold the larger floor, and either can be listed first:
    // the larger binds every time, and the other keeps its `minNotional` in the
    // report. On a tie `NOTIONAL`'s is taken.
    for (older, newer, binding, older_taken) in [
        ("10", "5", "10", true),
        ("5", "10", "10", false),
        ("5", "5", "5", false),
    ] {
        let (a, b) = (older_filter(older), newer_filter(newer));
        for filters in [format!("[{a}, {b}]"), format!("[{b}, {a}]")] {
            let got = ethbtc_from(&with_filters(&filters)).expect("parses");
            assert_eq!(got.definition.min_notional, notional(binding), "{filters}");
            for expected in [
                older_reported(older, older_taken),
                newer_reported(newer, !older_taken),
            ] {
                assert!(
                    got.unmodelled.contains(&expected),
                    "{filters}: wanted {expected:?} in {:?}",
                    got.unmodelled
                );
            }
        }
    }
}

/// Where [`REST_ANSWER`] lists its one rate limit, the placeholder.
const PLACEHOLDER_RATE_LIMITS: &str = "\"rateLimits\": [\n        {\n        }\n    ],\n";

#[test]
fn every_rate_limit_is_reported_whole_and_orders_is_one() {
    // `ORDERS` is the unfilled order rate limit, and this answer is where the
    // venue states it (`faqs/order_count_decrement.md` lines 16-18): every
    // order placed counts against it (line 37), and one past it is refused
    // with `-1015` (`errors.md` lines 43-45). A rule about orders the model
    // does not have, so it is reported, whole -- its values all distinct, so
    // a field dropped or swapped shows. The other limiter is reported as well,
    // with a field no page documents: nothing here decides which limit
    // matters.
    let answer = edit(
        REST_ANSWER,
        PLACEHOLDER_RATE_LIMITS,
        r#""rateLimits": [
            {"rateLimitType": "ORDERS", "interval": "SECOND", "intervalNum": 10, "limit": 50},
            {"rateLimitType": "RAW_REQUESTS", "interval": "MINUTE", "intervalNum": 5, "limit": 61000, "futureField": "x"}
        ],
"#,
    );
    let got = ethbtc_from(&answer).expect("parses");
    assert_eq!(
        got.unmodelled,
        vec![
            rate_limit(&[
                ("interval", "SECOND"),
                ("intervalNum", "10"),
                ("limit", "50"),
                ("rateLimitType", "ORDERS"),
            ]),
            rate_limit(&[
                ("futureField", "x"),
                ("interval", "MINUTE"),
                ("intervalNum", "5"),
                ("limit", "61000"),
                ("rateLimitType", "RAW_REQUESTS"),
            ]),
        ]
    );

    // And the other direction: absent or `null` is no rate limits, which *"All
    // limits are optional"* allows (`rest-api.md` line 819), not an error.
    for none in ["", "\"rateLimits\": null,\n"] {
        let got = ethbtc_from(&edit(REST_ANSWER, PLACEHOLDER_RATE_LIMITS, none))
            .expect("an answer without rate limits parses");
        assert_eq!(got.unmodelled, Vec::new(), "{none:?}");
    }
}

#[test]
fn a_reported_rule_prints_under_the_venues_name() {
    assert_eq!(
        rule(
            "MARKET_LOT_SIZE",
            &[("maxQty", "250"), ("minQty", "0"), ("stepSize", "0.1")]
        )
        .to_string(),
        "MARKET_LOT_SIZE maxQty=250 minQty=0 stepSize=0.1"
    );
    assert_eq!(
        exchange_rule("EXCHANGE_MAX_NUM_ORDERS", &[("maxNumOrders", "1000")]).to_string(),
        "EXCHANGE_MAX_NUM_ORDERS (exchange-wide) maxNumOrders=1000"
    );
    // A rate limit is not a filter: it prints under its list's name, and says
    // which limiter it is among its fields.
    assert_eq!(
        rate_limit(&[
            ("interval", "SECOND"),
            ("intervalNum", "10"),
            ("limit", "50"),
            ("rateLimitType", "ORDERS"),
        ])
        .to_string(),
        "rateLimits (exchange-wide) interval=SECOND intervalNum=10 limit=50 rateLimitType=ORDERS"
    );
    assert_eq!(rule("BARE_RULE", &[]).to_string(), "BARE_RULE");
}

/// How long the stand-in venue waits for a request before giving up.
const STAND_IN_PATIENCE: Duration = Duration::from_secs(10);

/// Answer one HTTP request on a loopback port with `status_line` and `body`,
/// and hand back the request line that arrived.
///
/// No test reaches the real venue, and none should. This stands in for it so
/// the request the client sends and what [`fetch`] makes of each kind of answer
/// can both be pinned.
///
/// It waits a bounded time, never forever. The client honours proxy settings
/// from the environment, and on a machine that sends loopback through a proxy
/// the request never arrives here: that should fail the test, with a request
/// line saying so, rather than hang it on an `accept` nothing will answer.
fn serve_once(status_line: &'static str, body: String) -> (String, JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let base = format!("http://{}", listener.local_addr().expect("its address"));
    let served = thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("a non-blocking listener");
        let deadline = Instant::now() + STAND_IN_PATIENCE;
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == ErrorKind::WouldBlock && Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(e) => return format!("no request arrived: {e}"),
            }
        };
        // An accepted socket may inherit the listener's mode; this one reads
        // and writes plainly, and still not forever.
        stream.set_nonblocking(false).expect("a blocking stream");
        stream
            .set_read_timeout(Some(STAND_IN_PATIENCE))
            .expect("a read timeout");
        let mut head = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).expect("the request");
            if read == 0 {
                break;
            }
            head.extend_from_slice(&chunk[..read]);
        }
        let response = format!(
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).expect("the response");
        String::from_utf8_lossy(&head)
            .lines()
            .next()
            .unwrap_or_default()
            .to_owned()
    });
    (base, served)
}

fn client(base: &str) -> SnapshotClient {
    SnapshotClient::new(base, Duration::from_secs(10)).expect("a client")
}

#[tokio::test]
async fn fetch_asks_for_one_symbol_and_reads_the_answer() {
    let (base, served) = serve_once("HTTP/1.1 200 OK", WS_ANSWER.to_owned());
    let got = fetch(&client(&base), "BNBBTC").await;
    assert_eq!(
        served.join().expect("the stand-in venue"),
        "GET /api/v3/exchangeInfo?symbol=BNBBTC HTTP/1.1"
    );
    assert_eq!(
        got,
        Ok(parse("BNBBTC", WS_ANSWER.as_bytes()).expect("parses"))
    );
}

#[tokio::test]
async fn a_failed_fetch_is_a_value_and_never_a_definition() {
    // The venue's refusal of a symbol it does not list (`rest-api.md` line
    // 794), with its documented body (`errors.md` lines 6-9). It arrives as
    // the request failing, with the status -- not as an answer that failed to
    // parse, which is what reading the error document would have made of it.
    let (base, served) = serve_once(
        "HTTP/1.1 400 Bad Request",
        r#"{"code": -1121, "msg": "Invalid symbol."}"#.to_owned(),
    );
    assert_eq!(
        fetch(&client(&base), "BTCUSTD").await,
        Err(FetchError::Request(SnapshotError::Status(400)))
    );
    served.join().expect("the stand-in venue");

    // Something that is not an answer at all.
    let (base, served) = serve_once("HTTP/1.1 200 OK", "<html>maintenance</html>".to_owned());
    assert!(matches!(
        fetch(&client(&base), "ETHBTC").await,
        Err(FetchError::Answer(ExchangeInfoError::NotExchangeInfo(_)))
    ));
    served.join().expect("the stand-in venue");

    // An answer naming a symbol that is not trading.
    let halted = edit(
        REST_ANSWER,
        "\"status\": \"TRADING\"",
        "\"status\": \"HALT\"",
    );
    let (base, served) = serve_once("HTTP/1.1 200 OK", halted);
    assert_eq!(
        fetch(&client(&base), "ETHBTC").await,
        Err(FetchError::Answer(ExchangeInfoError::NotTrading {
            symbol: "ETHBTC".to_owned(),
            status: "HALT".to_owned(),
        }))
    );
    served.join().expect("the stand-in venue");
}
