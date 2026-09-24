from .config import settings

def get_proxy_url_for(target_url: str) -> str:
    u = target_url.lower()
    is_https = u.startswith("https://") or u.startswith("wss://")
    is_http = u.startswith("http://") or u.startswith("ws://")

    all_proxy = settings.ALL_PROXY
    https_proxy = settings.HTTPS_PROXY
    http_proxy = settings.HTTP_PROXY

    if is_https:
        return https_proxy or all_proxy or ""
    if is_http:
        return http_proxy or all_proxy or ""

    return all_proxy or https_proxy or http_proxy or ""
