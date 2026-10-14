from unittest.mock import Mock, patch

from robots_checker import (
    RobotsFetchError,
    analyze_robots,
    fetch_robots,
    normalize_robots_url,
    test_url as check_url,
)


def test_normalize_domain_and_direct_robots_url():
    assert normalize_robots_url("example.com") == "https://example.com/robots.txt"
    assert normalize_robots_url("https://example.com/catalog") == "https://example.com/robots.txt"
    assert normalize_robots_url("https://example.com/robots.txt") == "https://example.com/robots.txt"
    assert normalize_robots_url("https://example.com/folder/robots.txt?draft=1") == "https://example.com/robots.txt"
    assert normalize_robots_url("https://example.com/Robots.txt") == "https://example.com/robots.txt"


def test_fetch_error_keeps_blocked_url_and_status_for_manual_fallback():
    response = Mock(
        status_code=403,
        is_redirect=False,
        is_permanent_redirect=False,
        headers={},
    )
    with patch("robots_checker.fetcher._validate_public_host"), patch(
        "robots_checker.fetcher.requests.get", return_value=response
    ):
        try:
            fetch_robots("https://example.com/robots.txt")
        except RobotsFetchError as exc:
            assert exc.status_code == 403
            assert exc.url == "https://example.com/robots.txt"
            response.close.assert_called_once()
        else:
            raise AssertionError("Expected RobotsFetchError")


def test_fetcher_keeps_html_for_google_style_line_parsing():
    response = Mock(
        status_code=200,
        is_redirect=False,
        is_permanent_redirect=False,
        headers={"Content-Type": "text/html; charset=utf-8"},
    )
    response.iter_content.return_value = [b"<html>\nUser-agent: *\nDisallow: /private\n</html>"]
    with patch("robots_checker.fetcher._validate_public_host"), patch(
        "robots_checker.fetcher.requests.get", return_value=response
    ):
        fetched = fetch_robots("https://example.com/robots.txt")

    assert "User-agent" in fetched.content
    assert fetched.content_type == "text/html; charset=utf-8"


def test_parser_builds_groups_and_reports_line_meanings():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /admin/\nAllow: /admin/public/\nSitemap: https://example.com/sitemap.xml"
    )

    assert len(analysis.groups) == 1
    assert len(analysis.rules) == 2
    assert analysis.sitemaps == ["https://example.com/sitemap.xml"]
    assert "Забороняє" in analysis.lines[1].explanation
    assert not [issue for issue in analysis.issues if issue.severity == "error"]


def test_specific_user_agent_group_wins_over_wildcard_group():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /private/\n\n"
        "User-agent: Googlebot\nAllow: /private/"
    )

    google = check_url(analysis, "/private/report", "Googlebot")
    bing = check_url(analysis, "/private/report", "bingbot")

    assert google.allowed is True
    assert google.matched_rule.line == 5
    assert bing.allowed is False
    assert bing.matched_rule.line == 2


def test_more_specific_image_bot_group_wins_over_googlebot_group():
    analysis = analyze_robots(
        "User-agent: Googlebot\nAllow: /images/\n"
        "User-agent: Googlebot-Image\nDisallow: /images/"
    )

    assert check_url(analysis, "/images/photo.jpg", "Googlebot").allowed is True
    image_result = check_url(analysis, "/images/photo.jpg", "Googlebot-Image")
    assert image_result.allowed is False
    assert image_result.matched_rule.line == 4


def test_longest_matching_rule_wins_and_allow_wins_equal_length():
    analysis = analyze_robots(
        "User-agent: *\n"
        "Disallow: /catalog/\n"
        "Allow: /catalog/open/\n"
        "Disallow: /same\n"
        "Allow: /same"
    )

    assert check_url(analysis, "/catalog/closed/item", "Googlebot").allowed is False
    opened = check_url(analysis, "/catalog/open/item", "Googlebot")
    assert opened.allowed is True
    assert opened.matched_rule.line == 3
    assert check_url(analysis, "/same-page", "Googlebot").allowed is True


def test_wildcard_and_end_anchor_are_applied_to_path_and_query():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /*?sort=*\nAllow: /catalog/public$"
    )

    assert check_url(analysis, "/catalog/item?sort=price", "Googlebot").allowed is False
    assert check_url(analysis, "/catalog/public", "Googlebot").allowed is True
    assert check_url(analysis, "/catalog/public/child", "Googlebot").allowed is True


def test_leading_wildcard_is_supported_for_google_and_matches_urls():
    pattern = "*/f/*/*=*,*"
    analysis = analyze_robots(
        f"User-agent: *\nDisallow: {pattern}",
        profile="google",
    )

    assert not [issue for issue in analysis.issues if issue.severity == "error"]
    assert check_url(
        analysis,
        "/catalog/f/filter/brand=nike,adidas",
        "Googlebot",
    ).allowed is False


def test_leading_wildcard_is_valid_in_standard_mode():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: *.pdf$",
        profile="standard",
    )

    assert not [issue for issue in analysis.issues if issue.severity == "error"]
    assert not [issue for issue in analysis.issues if issue.severity == "warning"]
    assert check_url(analysis, "/documents/report.pdf", "ExampleBot").allowed is False


def test_empty_allow_is_visible_as_warning_but_empty_disallow_is_informational():
    analysis = analyze_robots(
        "User-agent: *\nAllow:\nDisallow:",
        profile="google",
    )

    allow_issues = analysis.lines[1].issues
    disallow_issues = analysis.lines[2].issues
    assert any(issue.severity == "warning" for issue in allow_issues)
    assert any("нічого не відкриває" in issue.message for issue in allow_issues)
    assert all(issue.severity == "info" for issue in disallow_issues)


def test_multiple_groups_for_same_agent_are_merged():
    analysis = analyze_robots(
        "User-agent: Googlebot\nDisallow: /one/\n"
        "User-agent: Bingbot\nDisallow: /bing/\n"
        "User-agent: Googlebot\nDisallow: /two/"
    )

    assert check_url(analysis, "/one/page", "Googlebot").allowed is False
    assert check_url(analysis, "/two/page", "Googlebot").allowed is False
    assert check_url(analysis, "/bing/page", "Googlebot").allowed is True


def test_invalid_and_engine_specific_directives_are_reported():
    analysis = analyze_robots(
        "Disallow: /before-agent/\n"
        "User-agent: *\n"
        "Disallow private\n"
        "Noindex: /secret/\n"
        "Crawl-delay: 10",
        profile="google",
    )

    messages = [issue.message for issue in analysis.issues]
    assert any("до першого User-agent" in message for message in messages)
    assert any("без двокрапки" in message for message in messages)
    assert any("Noindex" in message for message in messages)
    assert any("Google не підтримує Crawl-delay" in message for message in messages)


def test_google_rule_priority_counts_wildcards_and_end_anchor():
    wildcard = analyze_robots(
        "User-agent: *\nAllow: /page\nDisallow: /*.htm",
        profile="google",
    )
    anchored = analyze_robots(
        "User-agent: *\nAllow: /foo\nDisallow: /foo$",
        profile="google",
    )

    assert check_url(wildcard, "/page.htm", "Googlebot").allowed is False
    assert check_url(anchored, "/foo", "Googlebot").allowed is False
    assert check_url(anchored, "/foobar", "Googlebot").allowed is True


def test_percent_encoding_and_utf8_are_compared_by_profile():
    standard = analyze_robots(
        "User-agent: *\nDisallow: /café\nDisallow: /encoded/%62az",
        profile="standard",
    )
    google = analyze_robots(
        "User-agent: *\nDisallow: /café\nDisallow: /encoded/%62az",
        profile="google",
    )

    assert check_url(standard, "/caf%C3%A9", "ExampleBot").allowed is False
    assert check_url(standard, "/café", "ExampleBot").allowed is False
    assert check_url(standard, "/encoded/baz", "ExampleBot").allowed is False
    assert check_url(google, "/caf%C3%A9", "Googlebot").allowed is False
    assert check_url(google, "/encoded/baz", "Googlebot").allowed is True
    assert check_url(google, "/encoded/%62az", "Googlebot").allowed is False


def test_percent_encoded_special_characters_are_literal():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /files/a%2Ab%24",
        profile="standard",
    )

    assert check_url(analysis, "/files/a*b$", "ExampleBot").allowed is False
    assert check_url(analysis, "/files/axxbz", "ExampleBot").allowed is True


def test_robots_txt_itself_is_implicitly_allowed():
    analysis = analyze_robots("User-agent: *\nDisallow: /", profile="standard")

    result = check_url(analysis, "/robots.txt", "ExampleBot")
    assert result.allowed is True
    assert result.matched_rule is None


def test_rules_apply_only_to_same_scheme_host_and_port():
    analysis = analyze_robots("User-agent: *\nDisallow: /private", profile="google")
    robots_url = "https://example.com/robots.txt"

    assert check_url(analysis, "https://example.com/private", "Googlebot", robots_url).allowed is False
    assert check_url(analysis, "https://example.com:443/private", "Googlebot", robots_url).allowed is False
    outside = check_url(analysis, "http://example.com/private", "Googlebot", robots_url)
    assert outside.allowed is True
    assert "не застосовується" in outside.reason


def test_bom_at_start_is_ignored():
    analysis = analyze_robots("\ufeffUser-agent: *\nDisallow: /private", profile="google")

    assert check_url(analysis, "/private/page", "Googlebot").allowed is False


def test_google_lenient_syntax_is_applied_with_warnings():
    analysis = analyze_robots(
        "Useragent: Googlebot/2.1\nDisallow /private",
        profile="google",
    )

    assert check_url(analysis, "/private", "Googlebot").allowed is False
    assert len([issue for issue in analysis.issues if issue.severity == "warning"]) >= 2


def test_other_records_do_not_split_google_groups():
    analysis = analyze_robots(
        "User-agent: AlphaBot\n"
        "Sitemap: https://example.com/sitemap.xml\n"
        "Crawl-delay: 5\n"
        "User-agent: BetaBot\n"
        "Disallow: /private",
        profile="google",
    )

    assert len(analysis.groups) == 1
    assert check_url(analysis, "/private", "AlphaBot").allowed is False
    assert check_url(analysis, "/private", "BetaBot").allowed is False


def test_bing_crawl_delay_splits_groups_and_is_validated():
    analysis = analyze_robots(
        "User-agent: AlphaBot\n"
        "Crawl-delay: 5\n"
        "User-agent: BetaBot\n"
        "Disallow: /private",
        profile="bing",
    )
    invalid = analyze_robots("User-agent: bingbot\nCrawl-delay: 21", profile="bing")

    assert len(analysis.groups) == 2
    assert check_url(analysis, "/private", "AlphaBot").allowed is True
    assert check_url(analysis, "/private", "BetaBot").allowed is False
    assert any("від 1 до 20" in issue.message for issue in invalid.issues)


def test_googlebot_family_and_bing_legacy_fallbacks():
    google = analyze_robots(
        "User-agent: Googlebot\nDisallow: /shared\n"
        "User-agent: Googlebot-Image\nDisallow: /images",
        profile="google",
    )
    bing = analyze_robots("User-agent: msnbot\nDisallow: /legacy", profile="bing")
    google_fallback = analyze_robots("User-agent: Googlebot\nDisallow: /shared", profile="google")

    assert check_url(google, "/shared", "Googlebot-Image").allowed is True
    assert check_url(google, "/images", "Googlebot-Image").allowed is False
    assert check_url(google_fallback, "/shared", "Googlebot-Image").allowed is False
    assert check_url(google_fallback, "/shared", "Google-InspectionTool").allowed is False
    assert check_url(bing, "/legacy", "bingbot").allowed is False


def test_applebot_uses_googlebot_group_only_as_a_fallback():
    fallback = analyze_robots(
        "User-agent: Googlebot\nDisallow: /shared",
        profile="apple",
    )
    specific = analyze_robots(
        "User-agent: Googlebot\nDisallow: /shared\n"
        "User-agent: Applebot\nAllow: /shared",
        profile="apple",
    )
    crawl_delay = analyze_robots(
        "User-agent: Applebot\nCrawl-delay: 5",
        profile="apple",
    )

    assert check_url(fallback, "/shared", "Applebot").allowed is False
    assert check_url(specific, "/shared", "Applebot").allowed is True
    assert any("Applebot не підтримує" in issue.message for issue in crawl_delay.issues)


def test_google_adsbot_does_not_use_global_group():
    analysis = analyze_robots("User-agent: *\nDisallow: /", profile="google")

    assert check_url(analysis, "/landing", "AdsBot-Google").allowed is True


def test_google_index_html_allow_also_allows_directory_url():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /\nAllow: /public/index.html",
        profile="google",
    )

    assert check_url(analysis, "/public/", "Googlebot").allowed is True
    assert check_url(analysis, "/public/index.html", "Googlebot").allowed is True


def test_google_ignores_content_after_500_kibibytes():
    oversized = "User-agent: *\nAllow: /\n" + ("# padding\n" * 60000) + "Disallow: /late"
    analysis = analyze_robots(oversized, profile="google")

    assert check_url(analysis, "/late", "Googlebot").allowed is True
    assert any("500 КіБ" in issue.message for issue in analysis.issues)


def test_seo_risk_flags_open_checkout_account_and_search_paths():
    analysis = analyze_robots(
        "User-agent: *\n"
        "Allow: /checkout\n"
        "Allow: /customer/account/\n"
        "Allow: /search?query=*",
        profile="google",
    )
    risks = [issue for issue in analysis.issues if issue.severity == "risk"]

    assert [risk.line for risk in risks] == [2, 3, 4]
    assert any("кошика або оформлення" in risk.message for risk in risks)
    assert any("особистого кабінету" in risk.message for risk in risks)
    assert any("пошуку, фільтрів" in risk.message for risk in risks)


def test_seo_risk_recognizes_localized_common_paths():
    analysis = analyze_robots(
        "User-agent: *\nAllow: /кошик\nDisallow: /каталог/",
        profile="google",
    )

    assert len([issue for issue in analysis.issues if issue.severity == "risk"]) == 2


def test_seo_risk_flags_broad_content_and_resource_blocks():
    analysis = analyze_robots(
        "User-agent: *\n"
        "Disallow: /catalog/\n"
        "Disallow: /*.css$\n"
        "Disallow: /images/",
        profile="google",
    )
    risks = [issue for issue in analysis.issues if issue.severity == "risk"]

    assert [risk.line for risk in risks] == [2, 3, 4]
    assert any("важливий контент" in risk.message for risk in risks)
    assert any("CSS, JavaScript" in risk.message for risk in risks)
    assert any("зображення" in risk.message for risk in risks)


def test_seo_risk_does_not_flag_narrow_catalog_filter_block():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /catalog/filter/",
        profile="google",
    )

    assert not [issue for issue in analysis.issues if issue.severity == "risk"]


def test_seo_risk_does_not_flag_allowed_rendering_resources():
    analysis = analyze_robots(
        "User-agent: *\n"
        "Disallow: /wp-admin/\n"
        "Allow: /wp-admin/admin-ajax.php\n"
        "Allow: /account/styles.css",
        profile="google",
    )

    assert not [issue for issue in analysis.issues if issue.severity == "risk"]


def test_seo_risk_flags_full_site_block_only_for_indexing_agents():
    global_block = analyze_robots("User-agent: *\nDisallow: /", profile="google")
    ai_block = analyze_robots("User-agent: GPTBot\nDisallow: /", profile="google")

    assert any(issue.severity == "risk" for issue in global_block.issues)
    assert not [issue for issue in ai_block.issues if issue.severity == "risk"]


def test_seo_risk_explains_equal_allow_disallow_conflict():
    analysis = analyze_robots(
        "User-agent: *\nDisallow: /checkout\nAllow: /checkout",
        profile="google",
    )
    conflict_risks = [
        issue
        for issue in analysis.issues
        if issue.severity == "risk" and "є і Allow, і Disallow" in issue.message
    ]

    assert len(conflict_risks) == 1
    assert conflict_risks[0].line == 3
    assert "перемагає Allow" in conflict_risks[0].message
