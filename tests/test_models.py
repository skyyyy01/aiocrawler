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


class TestReplaceIsolation:
    """回归：replace() 曾与原请求共享 meta / headers 这两个 dict。"""

    def test_meta_and_headers_are_copied(self):
        original = Request("https://x.com", meta={"page": 1}, headers={"A": "1"})
        derived = original.replace(retries=1)

        derived.meta["page"] = 99
        derived.headers["A"] = "changed"
        # 改派生请求不能反过来影响原请求，否则「重试改个 header
        # 结果原请求跟着变」这类问题在并发下根本查不出来
        assert original.meta == {"page": 1}
        assert original.headers == {"A": "1"}

    def test_explicit_meta_still_wins(self):
        original = Request("https://x.com", meta={"page": 1})
        assert original.replace(meta={"page": 2}).meta == {"page": 2}


class TestInternalMetaNotSerialized:
    """回归：下划线开头的内部记账字段不得写进队列 payload。"""

    def test_internal_keys_stripped(self):
        r = Request("https://x.com", meta={"page": 3, "_queue_id": 7})
        assert r.to_dict()["meta"] == {"page": 3}
        assert Request.from_json(r.to_json()).meta == {"page": 3}

    def test_redis_member_does_not_nest(self):
        """Redis 的 member 里存着整条 payload，跟着重试再序列化就会层层嵌套。"""
        r = Request("https://example.com/a")
        sizes = []
        for i in range(5):
            r.meta["_redis_member"] = f"{i}|{r.to_json()}"
            r = r.replace(retries=r.retries + 1, dont_filter=True)
            sizes.append(len(r.to_json()))
        # 每轮大小应当持平，而不是滚雪球
        assert max(sizes) - min(sizes) < 20, sizes

    def test_auto_proxy_credentials_not_persisted(self):
        r = Request("https://x.com", meta={"_proxy": "http://user:secret@p:8080"})
        assert "secret" not in r.to_json()
