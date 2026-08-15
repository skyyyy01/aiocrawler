"""Request / Response 的核心行为。

指纹与序列化是分布式升级的地基，必须有测试守住。
"""

from __future__ import annotations

from aiocrawler.models import Request, Response, canonicalize_url


class TestCanonicalizeUrl:
    def test_query_order_irrelevant(self):
        assert canonicalize_url("http://a.com/p?b=2&a=1") == canonicalize_url("http://a.com/p?a=1&b=2")

    def test_tracking_params_stripped(self):
        assert canonicalize_url("http://a.com/p?a=1&utm_source=x&gclid=y") == canonicalize_url("http://a.com/p?a=1")

    def test_fragment_dropped(self):
        # #anchor 不会改变服务端响应，应视为同一页面
        assert canonicalize_url("http://a.com/p#sec1") == canonicalize_url("http://a.com/p")

    def test_scheme_and_host_lowercased(self):
        assert canonicalize_url("HTTP://A.COM/p") == canonicalize_url("http://a.com/p")

    def test_empty_path_becomes_root(self):
        assert canonicalize_url("http://a.com") == canonicalize_url("http://a.com/")

    def test_path_case_preserved(self):
        # 路径大小写敏感，不能一并小写
        assert canonicalize_url("http://a.com/AbC") != canonicalize_url("http://a.com/abc")


class TestFingerprint:
    def test_equivalent_urls_share_fingerprint(self):
        a = Request("http://a.com/p?x=1&utm_medium=ad")
        b = Request("http://a.com/p?x=1")
        assert a.fingerprint() == b.fingerprint()

    def test_method_affects_fingerprint(self):
        assert Request("http://a.com/", method="GET").fingerprint() != \
               Request("http://a.com/", method="POST").fingerprint()

    def test_body_affects_fingerprint(self):
        assert Request("http://a.com/", method="POST", body=b"a").fingerprint() != \
               Request("http://a.com/", method="POST", body=b"b").fingerprint()


class TestSerialization:
    def test_roundtrip_preserves_all_fields(self):
        original = Request(
            url="http://a.com/p",
            callback="parse_detail",
            method="POST",
            headers={"X-Test": "1"},
            body=b"payload",
            meta={"page": 3, "tag": "中文"},
            priority=5,
            renderer="browser",
            dont_filter=True,
            retries=2,
        )
        restored = Request.from_json(original.to_json())
        assert restored == original

    def test_callback_is_plain_string(self):
        # 这是分布式可序列化的前提：callback 绝不能变成函数引用
        assert isinstance(Request("http://a.com/", callback="parse").callback, str)


class TestResponse:
    def _resp(self, html: str, url: str = "http://a.com/dir/page.html") -> Response:
        return Response(url=url, status=200, headers={}, body=html.encode(), request=Request(url))

    def test_text_helpers(self):
        r = self._resp('<h1> Hello </h1><a href="x.html" title="T">link</a>')
        assert r.text_of("h1") == "Hello"
        assert r.attr_of("a", "title") == "T"

    def test_missing_selector_returns_default(self):
        r = self._resp("<p>x</p>")
        assert r.text_of("h1") == ""
        assert r.text_of("h1", "缺省") == "缺省"
        assert r.attr_of("a", "href", "none") == "none"

    def test_urljoin_uses_final_url(self):
        r = self._resp("<html></html>")
        assert r.urljoin("x.html") == "http://a.com/dir/x.html"
        assert r.urljoin("/x.html") == "http://a.com/x.html"

    def test_follow_builds_absolute_request(self):
        r = self._resp("<html></html>")
        req = r.follow("next.html", callback="parse_next", priority=2)
        assert req.url == "http://a.com/dir/next.html"
        assert req.callback == "parse_next"
        assert req.priority == 2

    def test_meta_passthrough(self):
        req = Request("http://a.com/", meta={"depth": 2})
        resp = Response(url="http://a.com/", status=200, headers={}, body=b"", request=req)
        assert resp.meta["depth"] == 2
