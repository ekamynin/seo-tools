from robots_checker import analyze_robots, normalize_robots_url, test_url as check_url


def test_normalize_domain_and_direct_robots_url():
    assert normalize_robots_url("example.com") == "https://example.com/robots.txt"
    assert normalize_robots_url("https://example.com/catalog") == "https://example.com/robots.txt"
    assert normalize_robots_url("https://example.com/robots.txt") == "https://example.com/robots.txt"


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
    assert any("Немає двокрапки" in message for message in messages)
    assert any("Noindex" in message for message in messages)
    assert any("Google не підтримує Crawl-delay" in message for message in messages)
