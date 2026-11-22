import logging
from app.db.models.webhook_event import WebhookEvent
from app.core.exceptions import PermanentProcessingError

logger = logging.getLogger(__name__)

def handle_order_created(event: WebhookEvent)->None:
    
    data = event.payload.get("data")
    
    if not isinstance(data,dict):
        raise PermanentProcessingError("Invalid order data")
    
    order_id = data.get("order_id")
    amount = data.get("amount")
    
    if order_id is None or amount is None:
        raise PermanentProcessingError("Missing order id or amount in event payload")
    
    logger.info(
        "Processing order.creader : order_id=%s, amount=%s", order_id, amount
    )
    
    