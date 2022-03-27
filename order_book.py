from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    sender_pk = order['sender_pk']
    receiver_pk = order['receiver_pk']
    buy_currency = order['buy_currency']
    sell_currency = order['sell_currency']
    buy_amount = order['buy_amount']
    sell_amount = order['sell_amount']

    # add to sessions
    new_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency,
                      sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, filled=None)
    session.add(new_order)
    session.commit()

    # check if there are any existing order that matches
    while (new_order is not None):
        for existing_order in session.query(Order).filter(Order.filled == None).all():
            # if a good rate found,set to filled
            if existing_order.filled == None and existing_order.buy_currency == new_order.sell_currency and \
                    existing_order.sell_currency == new_order.buy_currency and \
                    existing_order.sell_amount / existing_order.buy_amount >= new_order.buy_amount / new_order.sell_amount:

                new_order.filled = datetime.now()
                existing_order.filled = datetime.now()
                existing_order.counterparty_id = new_order.id
                new_order.counterparty_id = existing_order.id

                if new_order.buy_amount < existing_order.sell_amount:
                    remaining_sell = existing_order.sell_amount - new_order.buy_amount
                    remaining_buy =  remaining_sell/(existing_order.sell_amount / existing_order.buy_amount)
                    new_order = Order(sender_pk=existing_order.sender_pk, receiver_pk=existing_order.receiver_pk,
                                      buy_currency=existing_order.buy_currency,
                                      sell_currency=existing_order.sell_currency, buy_amount=remaining_buy,
                                      sell_amount=remaining_sell, creator_id=existing_order.id, filled=None)
                    session.add(new_order)
                    session.commit()
                elif new_order.buy_amount>existing_order.sell_amount :
                    remaining_buy = new_order.buy_amount - existing_order.sell_amount
                    remaining_sell = remaining_buy/(new_order.buy_amount/ new_order.sell_amount)
                    new_order = Order(sender_pk=new_order.sender_pk,receiver_pk=new_order.receiver_pk,
                                        buy_currency=new_order.buy_currency, sell_currency=new_order.sell_currency,
                                        sell_amount= remaining_sell, buy_amount=remaining_buy, creator_id=new_order.id, filled=None)
                    session.add(new_order)
                    session.commit()

                else:
                    # exact amount,no child
                    new_order = None
                break
        return
