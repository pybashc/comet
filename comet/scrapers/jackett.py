import asyncio

from comet.core.constants import INDEXER_TIMEOUT
from comet.core.logger import logger
from comet.core.models import settings
from comet.core.scrape import ScrapeContext
from comet.scrapers.base import (
    BaseScraper,
    deduplicate_torrents,
    gather_with_error_logging,
)
from comet.scrapers.helpers.indexer import resolve_indexer_release
from comet.scrapers.models import ScrapeRequest, ScrapeResult
from comet.services.indexer_manager import indexer_manager


class JackettScraper(BaseScraper):
    reports_entry_completion = True

    async def process_torrent(self, result: dict, media_id: str, season: int):
        return await resolve_indexer_release(
            self.session,
            title=result["Title"],
            seeders=result["Seeders"],
            size=result["Size"],
            tracker=result["Tracker"],
            info_hash=result.get("InfoHash"),
            magnet_url=result.get("MagnetUri"),
            download_url=result.get("Link"),
            grab_known_hash=settings.JACKETT_GRAB_KNOWN_HASHES,
            media_id=media_id,
            season=season,
            download_failures=self.download_failures,
        )

    async def fetch_jackett_results(self, indexer: str, query: str):
        async with self.session.get(
            f"{self.url}/api/v2.0/indexers/all/results",
            params={
                "apikey": settings.JACKETT_API_KEY,
                "Query": query,
                "Tracker[]": indexer,
            },
            timeout=INDEXER_TIMEOUT,
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            data = await response.json()
            if not isinstance(data, dict) or not isinstance(data.get("Results"), list):
                raise ValueError("response payload is missing a results list")
            return data["Results"]

    def _entry_key(self, indexer: str) -> str:
        return f"{self.name}:{indexer}"

    async def _scrape_indexer(
        self,
        indexer: str,
        request: ScrapeRequest,
        queries: tuple[str, ...],
        budget: float | None,
    ):
        try:
            async with asyncio.timeout(budget):
                result_sets = await gather_with_error_logging(
                    (
                        f"Jackett query {query!r} via indexer {indexer!r}",
                        self.fetch_jackett_results(indexer, query),
                    )
                    for query in queries
                )

                seen: set[str] = set()
                torrent_tasks = []
                for result_set in result_sets:
                    for result in result_set:
                        if not isinstance(result, dict):
                            continue
                        details = result.get("Details")
                        if (
                            not isinstance(details, str)
                            or not details
                            or details in seen
                        ):
                            continue

                        seen.add(details)
                        torrent_tasks.append(
                            (
                                f"Jackett result {details!r}",
                                self.process_torrent(
                                    result,
                                    request.media_only_id,
                                    request.season,
                                ),
                            )
                        )

                processed_torrents = await gather_with_error_logging(torrent_tasks)
        except TimeoutError:
            logger.debug(
                f"Jackett indexer {indexer!r} exceeded its {budget:g}s live budget"
            )
            return []

        if len(result_sets) == len(queries):
            self.completed_entries.add(self._entry_key(indexer))

        return [
            torrent
            for sublist in processed_torrents
            for torrent in sublist
            if isinstance(torrent, dict) and torrent.get("infoHash")
        ]

    async def scrape(self, request: ScrapeRequest):
        if not settings.JACKETT_INDEXERS:
            try:
                await asyncio.wait_for(
                    indexer_manager.jackett_initialized.wait(),
                    timeout=settings.INDEXER_MANAGER_WAIT_TIMEOUT,
                )
            except TimeoutError:
                logger.warning(
                    "Timed out waiting for Jackett indexers; skipping scrape."
                )
                return []

        if not settings.JACKETT_INDEXERS:
            logger.warning("No Jackett indexers available, skipping scrape.")
            return []

        configured_indexers = list(settings.JACKETT_INDEXERS)
        indexers = [
            indexer
            for indexer in configured_indexers
            if self._entry_key(indexer) not in request.skip_scrapers
        ]
        if not indexers:
            return []

        budget = (
            settings.INDEXER_LIVE_TIMEOUT
            if request.context == ScrapeContext.LIVE
            and settings.INDEXER_LIVE_TIMEOUT > 0
            else None
        )
        queries = request.title_queries(include_episode_variants=True)

        torrents: list[ScrapeResult] = []
        try:
            per_indexer_results = await gather_with_error_logging(
                (
                    f"Jackett indexer {indexer!r}",
                    self._scrape_indexer(indexer, request, queries, budget),
                )
                for indexer in indexers
            )
            for sublist in per_indexer_results:
                torrents.extend(sublist)
        except Exception as e:
            logger.warning(
                f"Exception while getting torrents for {request.title} with Jackett: {e}"
            )

        if self.name and indexers == configured_indexers and all(
            self._entry_key(indexer) in self.completed_entries
            for indexer in configured_indexers
        ):
            self.completed_entries.add(self.name)

        self.log_download_failures()
        return deduplicate_torrents(torrents)
