"""The authentication extension point. A no-op in v1 (Q6): expose the service on a trusted network only.

A Standard-Webhooks signature verifier (``webhook-id`` / ``webhook-signature``) can be added here
without changing the API shape.
"""

from fastapi import Request


async def authenticate(request: Request) -> None:
    return None
