"""配置分层合并。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from aiocrawler.engine import Engine
from aiocrawler.settings import Settings, load_settings, read_config_file
from aiocrawler.spider import BaseSpider


def write_toml(tmp_path, content: str) -> str:
    path = tmp_path / "settings.toml"
    path.write_text(content, encoding="utf-8")
    return str(path)


class TestSettingsModel:
    def test_defaults(self):
        s = Settings()
        assert s.concurrency == 16
        assert s.respect_robots is True
        assert s.download_delay == 1.0

    def test_merged_returns_new_instance(self):
        base = Settings()
        merged = base.merged({"concurrency": 4})
        assert merged.concurrency == 4
        assert base.concurrency == 16   # 原对象不被修改

    def test_merged_with_none_is_identity(self):
        base = Settings()
        assert base.merged(None) is base

    def test_unknown_key_rejected(self):
        """拼错配置名应当立刻报错，而不是被静默忽略。"""
        with pytest.raises(ValidationError):
            Settings(concurency=4)   # 故意拼错

    def test_validation_bounds(self):
        with pytest.raises(ValidationError):
            Settings(concurrency=0)
        with pytest.raises(ValidationError):
            Settings(download_delay=-1)


class TestConfigFile:
    def test_missing_file_returns_empty(self, tmp_path):
        assert read_config_file(tmp_path / "nope.toml") == {}

    def test_reads_toml(self, tmp_path):
        path = write_toml(tmp_path, "[default]\nconcurrency = 3\n")
        assert read_config_file(path) == {"default": {"concurrency": 3}}


class TestLayering:
    def test_default_section_applied(self, tmp_path):
        path = write_toml(tmp_path, "[default]\nconcurrency = 3\n")
        assert load_settings(config_file=path).concurrency == 3

    def test_custom_settings_beat_default_section(self, tmp_path):
        path = write_toml(tmp_path, "[default]\nconcurrency = 3\n")
        s = load_settings("books", custom_settings={"concurrency": 7}, config_file=path)
        assert s.concurrency == 7

    def test_spider_section_beats_custom_settings(self, tmp_path):
        """运维在 toml 里的专属配置应能覆盖代码里的 custom_settings。"""
        path = write_toml(
            tmp_path,
            "[default]\nconcurrency = 3\n\n[spider.books]\nconcurrency = 9\n",
        )
        s = load_settings("books", custom_settings={"concurrency": 7}, config_file=path)
        assert s.concurrency == 9

    def test_cli_beats_everything(self, tmp_path):
        path = write_toml(
            tmp_path,
            "[default]\nconcurrency = 3\n\n[spider.books]\nconcurrency = 9\n",
        )
        s = load_settings(
            "books",
            custom_settings={"concurrency": 7},
            cli_overrides={"concurrency": 1},
            config_file=path,
        )
        assert s.concurrency == 1

    def test_other_spider_section_not_applied(self, tmp_path):
        path = write_toml(tmp_path, "[spider.other]\nconcurrency = 99\n")
        assert load_settings("books", config_file=path).concurrency == 16

    def test_layers_merge_per_field(self, tmp_path):
        """各层只覆盖自己声明的字段，未声明的沿用下层取值。"""
        path = write_toml(
            tmp_path,
            "[default]\nconcurrency = 3\ntimeout = 5.0\n\n[spider.books]\nconcurrency = 9\n",
        )
        s = load_settings("books", config_file=path)
        assert s.concurrency == 9      # 被 spider 段覆盖
        assert s.timeout == 5.0        # 沿用 default 段
        assert s.max_retries == 3      # 沿用内置默认

    def test_no_config_file_still_works(self, tmp_path):
        s = load_settings("books", custom_settings={"concurrency": 5},
                          config_file=tmp_path / "absent.toml")
        assert s.concurrency == 5


class TestProjectConfigFile:
    def test_shipped_settings_toml_is_valid(self):
        """仓库里自带的 settings.toml 必须能被正确解析并通过校验。"""
        s = load_settings("books", config_file="settings.toml")
        assert s.concurrency == 8
        assert s.download_delay == 0.1


class TestEngineDoesNotReapplyCustomSettings:
    """回归：Engine 曾经把 custom_settings 又合并一次，把命令行参数顶掉。

    load_settings() 已经按「custom_settings < [spider.x] < 命令行」叠好层，
    Engine 再叠一次就等于把 custom_settings 提到最高优先级——用户敲的
    `-c 32 --delay 5` 全部失效，而且没有任何提示。对 --delay 来说这尤其糟：
    想临时对生产站点降速，结果仍以代码里的间隔猛打。
    """

    class _Spider(BaseSpider):
        name = "demo"
        start_urls = []
        custom_settings = {"concurrency": 8, "download_delay": 0.1}

        async def parse(self, response):
            yield

    def test_cli_overrides_survive_engine_construction(self, tmp_path):
        path = write_toml(tmp_path, "[default]\nconcurrency = 3\n")
        spider = self._Spider()
        settings = load_settings(
            "demo",
            custom_settings=spider.custom_settings,
            cli_overrides={"concurrency": 32, "download_delay": 5.0},
            config_file=path,
        )
        engine = Engine(spider, settings, pipelines=[], middlewares=[])
        assert engine.settings.concurrency == 32
        assert engine.settings.download_delay == 5.0

    def test_spider_section_still_beats_custom_settings(self, tmp_path):
        path = write_toml(tmp_path, "[spider.demo]\nconcurrency = 9\n")
        spider = self._Spider()
        settings = load_settings(
            "demo", custom_settings=spider.custom_settings, config_file=path
        )
        engine = Engine(spider, settings, pipelines=[], middlewares=[])
        assert engine.settings.concurrency == 9

    def test_custom_settings_still_apply_without_load_settings(self):
        """直接构造 Settings 的老用法不能被破坏：这时 Engine 仍要负责合并。"""
        engine = Engine(self._Spider(), Settings(), pipelines=[], middlewares=[])
        assert engine.settings.concurrency == 8
