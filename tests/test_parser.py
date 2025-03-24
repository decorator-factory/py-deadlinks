from deadlinks.parser import analyze_page


EXAMPLE_HTML = """
<!doctype html>
<html>
    <head>
        <meta http-equiv="refresh" content="0; url=readme.html">
    </head>
    <body>
        <base href="https://example.su">
        <base href="https://example.su/test/">
        <base href="https://should-not-be-used.example.com/"/>
        <base target="_blank"/>

        <h1 id="heading">Heading</h1>
        Visit the <a href="https://example.com">example website</a>, or
        <a href="https://example.org/other#anchor">another one</a>, or
        <a href="foo.html">a relative one</a> and
        <a href="/foo/bar.html">another relative one</a>

        <h2 id="contact-me">Contact</h2>
        Send me spam at <a href="mailto:foo@bar.com">foo@bar.com</a>
    </body>
</html>
"""


def test_analyze_page():
    result = analyze_page(EXAMPLE_HTML)
    assert result.base_url == "https://example.su"
    assert set(result.ids) == {
        "heading",
        "contact-me",
    }
    assert set(result.links) == {
        "https://example.com",
        "https://example.org/other#anchor",
        "foo.html",
        "/foo/bar.html",
    }
    assert result.http_equiv_redirect == "readme.html"
    assert not result.warnings


def test_analyze_blank_page():
    result = analyze_page("")
    assert result.base_url is None
    assert set(result.ids) == set()
    assert set(result.links) == set()
    assert result.http_equiv_redirect is None
    assert not result.warnings
