from app.handlers.orders import handle_order_created
from app.handlers.register import register_handlers
from app.services.event_handlers import handlers


def test_order_handler_is_registered():
    
    register_handlers()
    
    assert handlers["order.created"] is handle_order_created