{
    "version": "1.7.2",
    "api_key": "M7G1koG6fNe61Ap5PGq3t5Eb7DnUG97C",
    "log_level": "DEBUG",
    "enable_network_tracing": true,
    "enable_stream_tracing": true,
    "retry_interval": 86400,
    "tracemalloc": false,
    "filesystem": {
        "mount_path": "/mnt/rivenfs",
        "library_profiles": {
            "anime": {
                "name": "Anime",
                "library_path": "/anime",
                "enabled": true,
                "filter_rules": {
                    "is_anime": true
                }
            },
            "example_kids": {
                "name": "Kids & Family Content",
                "library_path": "/kids",
                "enabled": false,
                "filter_rules": {
                    "content_types": [
                        "movie",
                        "show"
                    ],
                    "genres": [
                        "animation",
                        "family",
                        "!horror"
                    ],
                    "max_rating": 7.5,
                    "content_ratings": [
                        "G",
                        "PG",
                        "TV-Y",
                        "TV-Y7",
                        "TV-G",
                        "TV-PG"
                    ]
                }
            },
            "kids": {
                "name": "Kids & Family Content",
                "library_path": "/kids",
                "enabled": true,
                "filter_rules": {
                    "content_types": [
                        "movie",
                        "show"
                    ],
                    "genres": [
                        "animation",
                        "family",
                        "Adventure",
                        "Family"
                    ],
                    "max_rating": 7.5,
                    "content_ratings": [
                        "G",
                        "PG",
                        "TV-Y",
                        "TV-Y7",
                        "TV-G",
                        "TV-PG"
                    ]
                }
            }
        },
        "cache_dir": "/dev/shm/riven-cache",
        "cache_max_size_mb": 12600,
        "cache_ttl_seconds": 3600,
        "cache_eviction": "LRU",
        "cache_metrics": true,
        "movie_dir_template": "{title} ({year}) {{tmdb-{tmdb_id}}}",
        "movie_file_template": "{title} ({year})",
        "show_dir_template": "{title} ({year}) {{tvdb-{tvdb_id}}}",
        "season_dir_template": "Season {season:02d}",
        "episode_file_template": "{show[title]} - s{season:02d}e{episode:02d}"
    },
    "updaters": {
        "updater_interval": 90,
        "library_path": "/mnt/rivenfs",
        "plex": {
            "enabled": true,
            "token": "_vQuJvQbKszuWG9yrKpt",
            "url": "http://plex:32400"
        },
        "jellyfin": {
            "enabled": false,
            "api_key": "",
            "url": "http://localhost:8096"
        },
        "emby": {
            "enabled": false,
            "api_key": "",
            "url": "http://localhost:8096"
        }
    },
    "downloaders": {
        "video_extensions": [
            "mp4",
            "mkv",
            "avi"
        ],
        "movie_filesize_mb_min": 700,
        "movie_filesize_mb_max": -1,
        "episode_filesize_mb_min": 100,
        "episode_filesize_mb_max": -1,
        "proxy_url": "",
        "real_debrid": {
            "enabled": true,
            "api_key": "PXLXBFCSVZWD5HAJD6RJI6XWMLSA4S4AW64GZAL6QFHRQW2L5XZQ"
        },
        "debrid_link": {
            "enabled": false,
            "api_key": ""
        },
        "all_debrid": {
            "enabled": false,
            "api_key": ""
        }
    },
    "content": {
        "overseerr": {
            "update_interval": 60,
            "enabled": false,
            "url": "http://localhost:5055",
            "api_key": "",
            "use_webhook": false
        },
        "plex_watchlist": {
            "update_interval": 60,
            "enabled": true,
            "rss": []
        },
        "mdblist": {
            "update_interval": 86400,
            "enabled": false,
            "api_key": "",
            "lists": []
        },
        "listrr": {
            "update_interval": 86400,
            "enabled": false,
            "movie_lists": [],
            "show_lists": [],
            "api_key": ""
        },
        "trakt": {
            "update_interval": 86400,
            "enabled": true,
            "api_key": "28b52ec1714ff9fa5ed5591f6d09d4ff0dfd53ce9517ce65653fd3e241ba3862",
            "watchlist": [
                "coldspirit"
            ],
            "user_lists": [],
            "collection": [
                "coldspirit"
            ],
            "fetch_trending": false,
            "trending_count": 10,
            "fetch_popular": false,
            "popular_count": 10,
            "fetch_most_watched": false,
            "most_watched_period": "weekly",
            "most_watched_count": 10,
            "oauth": {
                "oauth_client_id": "28b52ec1714ff9fa5ed5591f6d09d4ff0dfd53ce9517ce65653fd3e241ba3862",
                "oauth_client_secret": "f073448ff0a3ab7f70968993d494bce08e220577169d8d35483fbba18ac2d748",
                "oauth_redirect_uri": "https://trakt.tv/oauth/authorize?response_type=code&client_id=9b36d8c0db59eff5038aea7a417d73e69aea75b41aac771816d2ef1b3109cc2f&redirect_uri=urn:ietf:wg:oauth:2.0:oob",
                "access_token": "",
                "refresh_token": ""
            },
            "proxy_url": ""
        }
    },
    "scraping": {
        "after_2": 2.0,
        "after_5": 6.0,
        "after_10": 24.0,
        "enable_aliases": true,
        "bucket_limit": 5,
        "max_failed_attempts": 0,
        "dubbed_anime_only": false,
        "torrentio": {
            "enabled": true,
            "filter": "sort=qualitysize%7Cqualityfilter=480p,scr,cam",
            "url": "http://torrentio.strem.fun",
            "timeout": 30,
            "retries": 1,
            "ratelimit": true,
            "proxy_url": ""
        },
        "jackett": {
            "enabled": false,
            "url": "http://localhost:9117",
            "api_key": "",
            "timeout": 30,
            "retries": 1,
            "infohash_fetch_timeout": 30,
            "ratelimit": true
        },
        "prowlarr": {
            "enabled": true,
            "url": "http://prowlarr:9696",
            "api_key": "c7eeec2d3ae5478e80826d4204261f73",
            "timeout": 45,
            "retries": 3,
            "infohash_fetch_timeout": 60,
            "ratelimit": true,
            "limiter_seconds": 60
        },
        "orionoid": {
            "enabled": true,
            "api_key": "QGGHHM8KE8FGNWFJ7UEKL5A8JMHJLFPR",
            "cached_results_only": false,
            "parameters": {
                "video3d": false,
                "videoquality": "sd_hd8k",
                "limitcount": 5
            },
            "timeout": 30,
            "retries": 1,
            "ratelimit": true
        },
        "mediafusion": {
            "enabled": false,
            "url": "http://localhost:8000",
            "timeout": 30,
            "retries": 1,
            "ratelimit": true
        },
        "zilean": {
            "enabled": true,
            "url": "http://zilean:8181",
            "timeout": 30,
            "retries": 1,
            "ratelimit": true
        },
        "comet": {
            "enabled": true,
            "url": "https://cometfortheweebs.midnightignite.me",
            "timeout": 30,
            "retries": 1,
            "ratelimit": true
        },
        "rarbg": {
            "enabled": true,
            "url": "https://therarbg.to",
            "timeout": 30,
            "retries": 1,
            "ratelimit": true
        },
        "aiostreams": {
            "enabled": false,
            "url": "http://localhost:8000",
            "timeout": 30,
            "retries": 1,
            "ratelimit": true,
            "proxy_url": "",
            "uuid": "",
            "password": ""
        }
    },
    "ranking": {
        "name": "example",
        "enabled": true,
        "require": [],
        "exclude": [
            "rus",
            "ita",
            "pol"
        ],
        "preferred": [
            "eng"
        ],
        "resolutions": {
            "r2160p": true,
            "r1080p": true,
            "r720p": true,
            "r480p": false,
            "r360p": false,
            "unknown": true
        },
        "options": {
            "title_similarity": 0.85,
            "remove_all_trash": true,
            "remove_ranks_under": -8500,
            "remove_unknown_languages": false,
            "allow_english_in_languages": true,
            "enable_fetch_speed_mode": true,
            "remove_adult_content": true
        },
        "languages": {
            "required": [],
            "allowed": [],
            "exclude": [
                "rus",
                "pol",
                "hun",
                "ita",
                "es",
                "ukr"
            ],
            "preferred": [
                "eng",
                "por"
            ]
        },
        "custom_ranks": {
            "quality": {
                "av1": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "avc": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "bluray": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dvd": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "hdtv": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "hevc": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "mpeg": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "remux": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "vhs": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "web": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "webdl": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "webmux": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "xvid": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                }
            },
            "rips": {
                "bdrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "brrip": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dvdrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "hdrip": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "ppvrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "satrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "tvrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "uhdrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "vhsrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "webdlrip": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "webrip": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                }
            },
            "hdr": {
                "bit10": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dolby_vision": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "hdr": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "hdr10plus": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "sdr": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                }
            },
            "audio": {
                "aac": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "atmos": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dolby_digital": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dolby_digital_plus": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dts_lossy": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dts_lossless": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "flac": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "mono": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "mp3": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "stereo": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "surround": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "truehd": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                }
            },
            "extras": {
                "three_d": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "converted": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "documentary": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "dubbed": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "edition": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "hardcoded": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "network": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "proper": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "repack": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "retail": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "site": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "subbed": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "upscaled": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "scene": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "uncensored": {
                    "fetch": true,
                    "use_custom_rank": false,
                    "rank": 0
                }
            },
            "trash": {
                "cam": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "clean_audio": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "pdtv": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "r5": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "screener": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "size": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "telecine": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                },
                "telesync": {
                    "fetch": false,
                    "use_custom_rank": false,
                    "rank": 0
                }
            }
        }
    },
    "indexer": {
        "schedule_offset_minutes": 30
    },
    "database": {
        "host": "postgresql+psycopg2://postgres:postgres@riven-db:5432/riven"
    },
    "notifications": {
        "enabled": true,
        "on_item_type": [
            "movie",
            "show",
            "season",
            "episode"
        ],
        "service_urls": []
    },
    "post_processing": {
        "subtitle": {
            "enabled": true,
            "languages": [
                "eng",
                "por"
            ],
            "providers": {
                "opensubtitles": {
                    "enabled": true
                }
            }
        }
    },
    "logging": {
        "enabled": true,
        "clean_interval": 3600,
        "retention_hours": 24,
        "rotation_mb": 10,
        "compression": "disabled"
    },
    "stream": {
        "chunk_size_mb": 80,
        "connect_timeout_seconds": 30,
        "chunk_wait_timeout_seconds": 300,
        "activity_timeout_seconds": 360
    }
}