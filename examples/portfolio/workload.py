"""Bounded storefront facts: real joined product changes and five-second bulk sales."""
TICKS_PER_SECOND = 2
MEMBERSHIP_SECONDS = 5
# IDs and cents are source facts, not presentation-generated rankings.
PRODUCTS = [
    (1,'Aero Desk Lamp','Workspace',12900),(2,'Arc Studio Headphones','Audio',18900),
    (3,'Forma Coffee Set','Kitchen',6800),(4,'Drift Weekender','Travel',14900),
    (5,'Field Notebook Trio','Everyday',2400),(6,'Mono Wireless Speaker','Audio',9900),
    (7,'Orbit Wall Clock','Home',7900),(8,'Terra Ceramic Planter','Home',4800),
    (9,'Loop Charging Dock','Workspace',8900),(10,'Slate Desk Mat','Workspace',6900),
    (11,'Cove Linen Throw','Home',11900),(12,'Roam Travel Set','Travel',6900),
    (13,'Sora Pour-over Kit','Kitchen',8400),(14,'North Bottle','Everyday',3900),
    (15,'Luma Reading Light','Home',5900),(16,'Tide Canvas Tote','Everyday',3400),
    (17,'Echo Pocket Radio','Audio',7400),(18,'Pico Task Timer','Workspace',2900),
    (19,'Dune Serving Tray','Kitchen',5400),(20,'Atlas Packing Cubes','Travel',4400),
]


class Phases:
    """Advance once per elapsed interval, without a burst after a slow commit."""
    def __init__(self, phase, now):
        self.phase, self.deadline = phase, now + MEMBERSHIP_SECONDS

    def current(self, now):
        if now >= self.deadline:
            self.phase += 1
            self.deadline = now + MEMBERSHIP_SECONDS
        return self.phase


def quote(value):
    return "'"+value.replace("'", "''")+"'"


def base_quantity(product):
    return (3000000-product[0]*75000)//product[3]


def initial_sql():
    catalog=','.join(f'({pid},{quote(name)},{quote(category)})' for pid,name,category,_ in PRODUCTS)
    lines=','.join(f'({pid*10+1},{pid},{base_quantity(product)},{price})' for product in PRODUCTS for pid,_,_,price in [product])
    return ('INSERT INTO public.demo_products(product_id,name,category) VALUES'+catalog+';'
            'INSERT INTO public.demo_order_lines(line_id,product_id,quantity,unit_price_cents) VALUES'+lines+';')


def changes_sql(tick, phase, previous_phase):
    statements=[]; events=[]
    if phase != previous_phase:
        statements.append('DELETE FROM public.demo_order_lines WHERE line_id BETWEEN 900000 AND 900020')
        if previous_phase is not None:
            events.append({'kind':'cancel','title':'Previous bulk line canceled','detail':'Its revenue retracts; the retained next product can refill.'})
        challenger=12 if phase%4==3 else 11+phase%10
        product=PRODUCTS[challenger-1];pid,name,_,price=product
        cutoff=base_quantity(PRODUCTS[9])*PRODUCTS[9][3]
        target=cutoff if phase%4==3 else 3100000
        quantity=max(1,(target-base_quantity(product)*price)//price)
        statements.append(f'INSERT INTO public.demo_order_lines VALUES({900000+pid},{pid},{quantity},{price})')
        events.append({'kind':'sale','title':f'{name} · bulk sale','detail':f'+{quantity} units','unit_price_cents':str(price),'product_id':pid})
        if phase%4==3:
            events.append({'kind':'tie','title':'An exact cutoff tie','detail':'Slate Desk Mat and Roam Travel Set tie on revenue; smaller product ID wins.'})
    product=PRODUCTS[tick%8];pid,name,category,price=product
    action=(tick//8)%4
    if action==3:
        statements.append(f'DELETE FROM public.demo_order_lines WHERE line_id={800000+pid}')
        events.append({'kind':'cancel','title':name+' · line canceled','detail':'DELETE retracts that line’s units and revenue.','product_id':pid})
    else:
        quantity=-1 if action==1 else 2 if action==0 else 3
        statements.append(f'INSERT INTO public.demo_order_lines VALUES({800000+pid},{pid},{quantity},{price}) ON CONFLICT(line_id) DO UPDATE SET quantity=EXCLUDED.quantity')
        events.append({'kind':'refund' if quantity<0 else 'sale','title':name+(' · refund' if quantity<0 else ' · sale'),
                       'detail':f'Line now {quantity:+d} units','unit_price_cents':str(price),'product_id':pid})
    # This catalog change and the fact mutation above share one source transaction.
    if phase != previous_phase:
        variant=name+(' / Sand' if phase%2 else '')
        statements.append(f'UPDATE public.demo_products SET name={quote(variant)},category={quote(category)} WHERE product_id={pid}')
        events.append({'kind':'catalog','title':variant+' · catalog update','detail':'Product metadata and an order line change in the same transaction.','product_id':pid})
    return ';'.join(statements)+';',events
