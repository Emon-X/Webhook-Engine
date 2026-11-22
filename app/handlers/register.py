from app.services.event_handlers import register_handler,handlers
from app.handlers.orders import handle_order_created


def register_handlers():
    
    event_type = "order.created"
    existing_handler = handlers.get(event_type)
    
    if existing_handler is None:
        register_handler(event_type, handle_order_created)
    
    elif existing_handler is not handle_order_created:
        raise ValueError(
            f"Conflicting handler registered for {event_type}"
        )