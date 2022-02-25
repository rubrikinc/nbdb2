"""
Utility to check Telegraf templating rules
"""

import argparse
import fnmatch
import os
import logging
import sys
from typing import List, Dict

import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAF_TEMPLATE_RULES_FILE = os.path.join(os.path.dirname(__file__),
                                            "telegraf_templating_rules.yml")
with open(TELEGRAF_TEMPLATE_RULES_FILE, 'r') as rules_file:
    TELEGRAF_TEMPLATE_RULES = yaml.safe_load(rules_file.read())


def parse_args(args):
    """
    Parse arguments
    """
    argparser = argparse.ArgumentParser(
        description='Check Telegraf template coverage')

    argparser.add_argument('--input', type=str, required=True,
                           help='Input file listing CDM series')
    argparser.add_argument('--whitelist_path', type=str, required=True,
                           help='Stats whitelist file')
    argparser.add_argument('--store_nonmatching', type=str,
                           help='Store metrics not matching Telegraf '
                           'template rules to given filename')
    args = argparser.parse_args()
    return args


def check_telegraf_templating_rules(metric: str) -> bool:
    """
    Checks if any templating rules match the given metric.

    :param metric: Graphite-style flattened series name
    :return: True if match was found, False otherwise.
    """
    for rule in TELEGRAF_TEMPLATE_RULES:
        if fnmatch.fnmatch(metric, rule['filter']):
            return True

    return False


def verify_telegraf_rule_sanity(rule: Dict) -> None:
    """
    Checks if the Telegraf templating rule is sane.
    :param: YAML rule
    """
    assert rule.get("filter"), "Filter pattern must be defined: %s" % rule
    assert rule.get("tag_rule"), "Tagging rule must be defined: %s" % rule
    # Each tagging rule must have one field
    assert "field" in rule["tag_rule"], \
        "Rule %s is missing 'field' in tag_rule" % rule['filter']

    if "field*" in rule["tag_rule"]:
        assert "measurement*" not in rule["tag_rule"], \
            "Rule %s cant have both 'field*' & 'measurement*'" % rule['filter']


def apply_template_rule(metric: str, rule: str) -> Dict:
    """
    Apply the tagging rule and generate its output

    Heavily inspired from
    https://github.com/influxdata/telegraf/blob/master/internal/templating/template.go
    """
    try:
        parts = metric.split(".")
        rule_parts = rule.split(" ")
        tags = None
        if len(rule_parts) > 1:
            additional_tags = rule_parts[1]
            additional_tag_parts = additional_tags.split(',')
            tags = dict()
            for additional_tag_part in additional_tag_parts:
                tag_parts = additional_tag_part.split('=')
                tags[tag_parts[0]] = [tag_parts[1]]
        template_parts = rule_parts[0].split(".")
        parts_idx = 0
        result = {'measurement': [], 'field': []}

        # Generate measurement, fields & tags according to given rule
        for templ_part in template_parts:
            if templ_part == 'measurement*':
                result['measurement'].extend(parts[parts_idx:])
                parts_idx = len(parts)
            elif templ_part == 'field*':
                result['field'].extend(parts[parts_idx:])
                parts_idx = len(parts)
            else:
                if templ_part in result:
                    result[templ_part] += [parts[parts_idx]]
                else:
                    result[templ_part] = [parts[parts_idx]]
                parts_idx += 1

        if tags is not None:
            result.update(tags)

        # Convert list values to "." separated strings
        for key in result:
            result[key] = ".".join(result[key])
        return result
    except:
        logger.exception("Failed to apply rule: %s to metric %s", rule, metric)


def check_stats_whitelist_rules(metric: str, whitelist_rules: List[Dict]) -> bool:
    """
    Checks if any stats whitelist rules match the given metric.

    :param metric: Graphite-style flattened series name
    :return: True if match was found, False otherwise.
    """
    for rule in whitelist_rules['telegraf_namepass']:
        if fnmatch.fnmatch(metric, rule['name']):
            return True

    return False


def report_coverage_all_metrics(args) -> None:
    """
    Print coverage stats for all listed metrics
    """
    # Load whitelist rules and metrics list
    whitelist_rules = []
    with open(args.whitelist_path, 'r') as whitelist_file:
        whitelist_rules = yaml.safe_load(whitelist_file.read())
        whitelist_rules = [w['name'] for w in
                           whitelist_rules['telegraf_namepass']]

    metrics = open(args.input, 'r').readlines()
    # Drop the first three tokens from each metric.
    # Eg. clusters.ABC.RVM123.Diamond.uptime is converted to Diamond.uptime
    metrics = set(".".join(m.strip().split(".")[3:]) for m in metrics)

    # Figure out how many metrics match the stats whitelist
    all_whitelisted_metrics = []
    for idx, rule in enumerate(whitelist_rules):
        if idx % 100 == 0:
            logger.info(
                "Processed %d WL rules. Matched WL: %d", idx,
                len(all_whitelisted_metrics))
        matching_metrics = fnmatch.filter(metrics, rule)
        all_whitelisted_metrics.extend(matching_metrics)

    # Figure out how many metrics match the Telegraf templating rules
    matched_metrics = set()
    matched_whitelist_metrics = set()
    measurements = set()
    for idx, rule in enumerate(TELEGRAF_TEMPLATE_RULES):
        # Verify rule sanity
        verify_telegraf_rule_sanity(rule)

        if idx % 10 == 0:
            logger.info(
                "Processed %d templ rules. Matched TT: %d, Matched TT+WL: %d",
                idx, len(matched_metrics), len(matched_whitelist_metrics))

        # Determine which metrics match the Telegraf template rules
        matching_metrics = set(fnmatch.filter(metrics, rule['filter']))
        clashing_metrics = matched_metrics.intersection(matching_metrics)
        if len(clashing_metrics) > 0:
            logger.warning(
                "Rule %s matches %d series already matched by prev rules",
                rule['filter'], len(clashing_metrics))

        matched_metrics.update(matching_metrics)
        if matching_metrics:
            result = apply_template_rule(matching_metrics.pop(),
                                         rule['tag_rule'])
            measurements.add(result['measurement'])
            logger.info("Sample output for rule %s: %s", rule['filter'],
                        result)

        # Also figure out the number of metrics which match both the stats
        # whitelist & Telegraf templating rules
        matching_wl_metrics = set(fnmatch.filter(all_whitelisted_metrics,
                                                 rule['filter']))
        matched_whitelist_metrics.update(matching_wl_metrics)

    if args.store_nonmatching:
        with open(args.store_nonmatching, 'w') as f:
            non_matching_metrics = list(metrics - matched_metrics)
            f.write("%s" % "\n".join(non_matching_metrics))

    print()
    print("TOTAL SERIES                          : %6s" % len(metrics))
    print("TOTAL WHITELISTED SERIES              : %6s" %
          len(all_whitelisted_metrics))
    print("TELEGRAF TEMPLATING MATCHED           : %6s (%.2f%%)" % (
        len(matched_metrics), len(matched_metrics) * 100.0 / len(metrics)))
    if len(all_whitelisted_metrics) > 0:
        print("TELEGRAF TEMPLATING MATCHED + WHITLIST: %6s (%.2f%%)" % (
          len(matched_whitelist_metrics),
          len(matched_whitelist_metrics) * 100.0 / len(all_whitelisted_metrics)))

    print()
    print("TOTAL DRUID MEASUREMENTS: %d" % len(measurements))

    print("Unmatched series")
    # Print the unmatched metrics
    for m in metrics:
        if m not in matched_metrics:
            print(m)


if __name__ == '__main__':
    sys_args = parse_args(sys.argv[1:])
    report_coverage_all_metrics(sys_args)
