from typing import Callable

from app.db.models.webhook_event import WebhookEvent

EventHandler = Callable[[WebhookEvent], None]
handlers : dict[str, EventHandler] = {}

def register_handler(event_type: str, handler: EventHandler):

    if event_type in handlers:
        raise ValueError(f"Handler for event type '{event_type}' is already registered.")
    
    handlers[event_type] = handler
    
def dispatch_event(event:WebhookEvent):

    handler = handlers.get(event.event_type)
    
    if handler is None:
        raise LookupError(f"No handler registered for event type '{event.event_type}'")
    
    handler(event)