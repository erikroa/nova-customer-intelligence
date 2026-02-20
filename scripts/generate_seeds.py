"""
Synthetic seed data for the Nova Customer Intelligence dbt project.
Creates 5 interconnected CSVs that simulate a real B2B SaaS company (NovaCRM).

Usage:
    pip install faker
    python generate_seeds.py

Output: 5 CSV files in a seeds/ folder
"""

import csv
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

os.makedirs("seeds", exist_ok=True)

# CONFIGURATION 
NUM_ACCOUNTS = 150
DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2025, 1, 31)

PLAN_TIERS = {
    "starter":    {"mrr": 49,  "weight": 0.45},
    "growth":     {"mrr": 149, "weight": 0.35},
    "enterprise": {"mrr": 499, "weight": 0.20},
}

ADDONS = {
    "api_access":          {"mrr": 29,  "chance": 0.25},
    "advanced_analytics":  {"mrr": 79,  "chance": 0.15},
    "priority_support":    {"mrr": 59,  "chance": 0.20},
}

INDUSTRIES = [
    "technology", "healthcare", "finance", "manufacturing",
    "retail", "education", "media", "logistics",
    "real_estate", "professional_services"
]

REGIONS = ["north_america", "europe", "apac", "latam"]

EVENT_NAMES = [
    "dashboard_viewed", "report_created", "contact_added",
    "email_sent", "deal_updated", "api_call", "export_generated",
    "workflow_created", "integration_configured", "user_invited",
    "search_performed", "filter_applied", "note_added",
    "task_completed", "meeting_logged"
]

TICKET_CATEGORIES = ["bug", "feature_request", "billing", "onboarding", "how_to", "performance"]
TICKET_PRIORITIES = ["p1", "p2", "p3", "p4"]

SLA_TARGETS = {"p1": 4, "p2": 12, "p3": 48, "p4": 120}


#generate accounts
print("Generating accounts...")

accounts = []
for i in range(1, NUM_ACCOUNTS + 1):
    account_id = f"ACC-{i:04d}"
    signup_date = fake.date_between(start_date=DATE_START, end_date=DATE_END - timedelta(days=60))

    # Weighted plan selection
    tier = random.choices(
        list(PLAN_TIERS.keys()),
        weights=[t["weight"] for t in PLAN_TIERS.values()],
        k=1
    )[0]

    # Employee count correlates with plan tier
    if tier == "enterprise":
        employee_count = random.randint(200, 5000)
    elif tier == "growth":
        employee_count = random.randint(30, 500)
    else:
        employee_count = random.randint(5, 100)

    # Status: most active, some churned, a few trials
    days_since_signup = (DATE_END - datetime.combine(signup_date, datetime.min.time())).days
    if days_since_signup < 30:
        status = random.choices(["trial", "active"], weights=[0.6, 0.4], k=1)[0]
    elif tier == "starter":
        status = random.choices(["active", "churned"], weights=[0.70, 0.30], k=1)[0]
    elif tier == "growth":
        status = random.choices(["active", "churned"], weights=[0.80, 0.20], k=1)[0]
    else:
        status = random.choices(["active", "churned", "suspended"], weights=[0.88, 0.08, 0.04], k=1)[0]

    accounts.append({
        "account_id": account_id,
        "company_name": fake.company(),
        "industry": random.choice(INDUSTRIES),
        "employee_count": employee_count,
        "plan_tier": tier,
        "account_owner": random.choice([
            "Sarah Chen", "Marcus Johnson", "Emily Rodriguez",
            "David Kim", "Rachel Thompson", "James O'Brien"
        ]),
        "region": random.choices(REGIONS, weights=[0.40, 0.30, 0.20, 0.10], k=1)[0],
        "signup_date": signup_date.isoformat(),
        "status": status,
    })

# Write accounts CSV
with open("seeds/raw_accounts.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=accounts[0].keys())
    writer.writeheader()
    writer.writerows(accounts)

print(f"  -> {len(accounts)} accounts")

#Generate Subscriptions

print("Generating subscriptions...")

subscriptions = []
sub_counter = 0

for acct in accounts:
    signup = datetime.fromisoformat(acct["signup_date"])

    # Base subscription (always exists)
    sub_counter += 1
    base_mrr = PLAN_TIERS[acct["plan_tier"]]["mrr"]

    if acct["status"] == "churned":
        # Churned accounts: subscription ended sometime after signup
        months_active = random.randint(2, 18)
        end_date = signup + timedelta(days=months_active * 30)
        if end_date > DATE_END:
            end_date = DATE_END - timedelta(days=random.randint(10, 60))
        sub_status = "cancelled"
    elif acct["status"] == "trial":
        end_date = signup + timedelta(days=14)
        sub_status = "trial"
        base_mrr = 0
    else:
        end_date = None
        sub_status = "active"

    subscriptions.append({
        "subscription_id": f"SUB-{sub_counter:05d}",
        "account_id": acct["account_id"],
        "product_name": "novacrm_platform",
        "plan_tier": acct["plan_tier"],
        "mrr_amount": base_mrr,
        "start_date": acct["signup_date"],
        "end_date": end_date.isoformat() if end_date else "",
        "status": sub_status,
    })

    # Some accounts upgraded mid-lifecycle
    if acct["status"] == "active" and acct["plan_tier"] != "enterprise" and random.random() < 0.15:
        sub_counter += 1
        upgrade_date = signup + timedelta(days=random.randint(90, 365))
        if upgrade_date < DATE_END:
            new_tier = "growth" if acct["plan_tier"] == "starter" else "enterprise"
            # Mark old sub as upgraded
            subscriptions[-1]["end_date"] = upgrade_date.isoformat()
            subscriptions[-1]["status"] = "upgraded"
            subscriptions.append({
                "subscription_id": f"SUB-{sub_counter:05d}",
                "account_id": acct["account_id"],
                "product_name": "novacrm_platform",
                "plan_tier": new_tier,
                "mrr_amount": PLAN_TIERS[new_tier]["mrr"],
                "start_date": upgrade_date.isoformat(),
                "end_date": "",
                "status": "active",
            })

    # Add-on subscriptions (only for active/churned, not trials)
    if acct["status"] != "trial":
        for addon_name, addon_info in ADDONS.items():
            # Enterprise more likely to have add-ons
            chance_modifier = 1.5 if acct["plan_tier"] == "enterprise" else 1.0
            if random.random() < addon_info["chance"] * chance_modifier:
                sub_counter += 1
                addon_start = signup + timedelta(days=random.randint(15, 180))
                if addon_start < DATE_END:
                    addon_end = None
                    addon_status = "active"
                    if acct["status"] == "churned":
                        addon_end = datetime.fromisoformat(subscriptions[-1]["end_date"]) if subscriptions[-1]["end_date"] else DATE_END
                        addon_status = "cancelled"

                    subscriptions.append({
                        "subscription_id": f"SUB-{sub_counter:05d}",
                        "account_id": acct["account_id"],
                        "product_name": addon_name,
                        "plan_tier": acct["plan_tier"],
                        "mrr_amount": addon_info["mrr"],
                        "start_date": addon_start.isoformat(),
                        "end_date": addon_end.isoformat() if addon_end else "",
                        "status": addon_status,
                    })

with open("seeds/raw_subscriptions.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=subscriptions[0].keys())
    writer.writeheader()
    writer.writerows(subscriptions)

print(f"  -> {len(subscriptions)} subscriptions")

# Generate Invoices

print("Generating invoices...")

invoices = []
inv_counter = 0

for acct in accounts:
    if acct["status"] == "trial":
        continue

    signup = datetime.fromisoformat(acct["signup_date"])
    acct_subs = [s for s in subscriptions if s["account_id"] == acct["account_id"]]

    # Generate monthly invoices from signup to end
    current_month = signup.replace(day=1)
    end = DATE_END

    while current_month < end:
        # Calculate total MRR for this month from active subs
        month_mrr = 0
        active_products = []
        for sub in acct_subs:
            sub_start = datetime.fromisoformat(sub["start_date"])
            sub_end = datetime.fromisoformat(sub["end_date"]) if sub["end_date"] else DATE_END + timedelta(days=365)
            if sub_start <= current_month < sub_end and sub["status"] != "trial":
                month_mrr += sub["mrr_amount"]
                active_products.append(sub["product_name"])

        if month_mrr > 0:
            inv_counter += 1

            # Most invoices paid, some overdue, rare voids
            if acct["status"] == "churned" and current_month > (end - timedelta(days=90)):
                inv_status = random.choices(["paid", "overdue", "void"], weights=[0.5, 0.3, 0.2], k=1)[0]
            else:
                inv_status = random.choices(["paid", "overdue"], weights=[0.92, 0.08], k=1)[0]

            invoice_date = current_month + timedelta(days=random.randint(0, 3))

            invoices.append({
                "invoice_id": f"INV-{inv_counter:06d}",
                "account_id": acct["account_id"],
                "invoice_date": invoice_date.isoformat()[:10],
                "amount": month_mrr,
                "currency": "USD",
                "status": inv_status,
                "line_items": "|".join(active_products),  # Pipe-separated for CSV friendliness
            })

        # Next month
        if current_month.month == 12:
            current_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            current_month = current_month.replace(month=current_month.month + 1)

with open("seeds/raw_invoices.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=invoices[0].keys())
    writer.writeheader()
    writer.writerows(invoices)

print(f"  -> {len(invoices)} invoices")

# Generate Usage Events

print("Generating usage events...")

usage_events = []
evt_counter = 0

# Build a lookup for account health (churned = declining usage)
account_status_map = {a["account_id"]: a["status"] for a in accounts}
account_tier_map = {a["account_id"]: a["plan_tier"] for a in accounts}
account_signup_map = {a["account_id"]: datetime.fromisoformat(a["signup_date"]) for a in accounts}

for acct in accounts:
    signup = account_signup_map[acct["account_id"]]
    tier = acct["plan_tier"]
    status = acct["status"]

    # Base daily event count by tier
    if tier == "enterprise":
        base_events_per_day = random.randint(8, 25)
        num_users = random.randint(3, 10)
    elif tier == "growth":
        base_events_per_day = random.randint(4, 15)
        num_users = random.randint(2, 5)
    else:
        base_events_per_day = random.randint(1, 8)
        num_users = random.randint(1, 3)

    user_ids = [f"{acct['account_id']}-U{u:02d}" for u in range(1, num_users + 1)]

    #Events for sampled days (not every day)
    # Sample around 2 days per week
    end_date = DATE_END if status != "churned" else signup + timedelta(days=random.randint(60, 500))
    if end_date > DATE_END:
        end_date = DATE_END

    total_days = (end_date - signup).days
    if total_days <= 0:
        continue

    # Sample days
    num_sample_days = min(total_days, max(20, total_days // 4))
    sample_day_offsets = sorted(random.sample(range(total_days), min(num_sample_days, total_days)))

    for day_offset in sample_day_offsets:
        event_date = signup + timedelta(days=day_offset)

        # Churned accounts: usage declines toward the end
        if status == "churned":
            progress = day_offset / total_days  # 0 = start, 1 = churn
            daily_events = max(1, int(base_events_per_day * (1 - progress * 0.8)))
        else:
            # Slight random variation
            daily_events = max(1, base_events_per_day + random.randint(-3, 3))

        for _ in range(daily_events):
            evt_counter += 1
            hour = random.choices(
                range(24),
                weights=[1,1,1,1,1,2,3,5,8,10,10,9,8,9,10,10,8,6,4,3,2,1,1,1],
                k=1
            )[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            event_ts = event_date.replace(hour=hour, minute=minute, second=second)

            # Enterprise accounts use more advanced features
            if tier == "enterprise":
                event_weights = [8,6,7,5,7,10,5,4,3,3,6,5,4,5,4]
            elif tier == "growth":
                event_weights = [10,7,8,6,6,4,4,3,2,2,7,6,5,5,3]
            else:
                event_weights = [12,5,10,4,5,1,2,1,1,1,8,7,3,4,2]

            event_name = random.choices(EVENT_NAMES, weights=event_weights, k=1)[0]

            usage_events.append({
                "event_id": f"EVT-{evt_counter:08d}",
                "account_id": acct["account_id"],
                "user_id": random.choice(user_ids),
                "event_name": event_name,
                "event_timestamp": event_ts.isoformat(),
                "properties": "",  
            })

# Cap at ~15000 events
if len(usage_events) > 15000:
    usage_events = sorted(usage_events, key=lambda x: x["event_timestamp"])
    usage_events = random.sample(usage_events, 15000)
    usage_events.sort(key=lambda x: x["event_timestamp"])

with open("seeds/raw_usage_events.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=usage_events[0].keys())
    writer.writeheader()
    writer.writerows(usage_events)

print(f"  -> {len(usage_events)} usage events")

#Generate Support Tickets
print("Generating support tickets...")

tickets = []
tkt_counter = 0

for acct in accounts:
    if acct["status"] == "trial":
        # Trials get 0-1 onboarding tickets
        if random.random() < 0.3:
            tkt_counter += 1
            created = datetime.fromisoformat(acct["signup_date"]) + timedelta(days=random.randint(1, 10))
            tickets.append({
                "ticket_id": f"TKT-{tkt_counter:05d}",
                "account_id": acct["account_id"],
                "created_at": created.isoformat(),
                "resolved_at": (created + timedelta(hours=random.randint(2, 48))).isoformat(),
                "priority": "p3",
                "category": "onboarding",
                "status": "resolved",
                "satisfaction_score": round(random.uniform(3.0, 5.0), 1),
            })
        continue

    signup = datetime.fromisoformat(acct["signup_date"])
    end = DATE_END

    # Ticket frequency depends on status and tier
    if acct["status"] == "churned":
        tickets_per_month = random.uniform(0.8, 2.5)  # Churning accounts file more tickets
    elif acct["plan_tier"] == "enterprise":
        tickets_per_month = random.uniform(0.3, 1.5)
    elif acct["plan_tier"] == "growth":
        tickets_per_month = random.uniform(0.2, 1.2)
    else:
        tickets_per_month = random.uniform(0.1, 0.8)

    months_active = max(1, (end - signup).days // 30)
    total_tickets = max(1, int(tickets_per_month * months_active))
    total_tickets = min(total_tickets, 25)  # Cap per account

    for _ in range(total_tickets):
        tkt_counter += 1
        created = fake.date_time_between(start_date=signup, end_date=end)

        priority = random.choices(
            TICKET_PRIORITIES,
            weights=[0.05, 0.15, 0.45, 0.35],
            k=1
        )[0]

        # Churned accounts: more bugs, less feature requests
        if acct["status"] == "churned":
            category = random.choices(
                TICKET_CATEGORIES,
                weights=[0.35, 0.10, 0.20, 0.10, 0.10, 0.15],
                k=1
            )[0]
        else:
            category = random.choices(
                TICKET_CATEGORIES,
                weights=[0.20, 0.20, 0.15, 0.15, 0.20, 0.10],
                k=1
            )[0]

        # Resolution time based on priority (with some misses)
        target_hours = SLA_TARGETS[priority]
        if random.random() < 0.15:  # 15% SLA breach
            resolution_hours = target_hours * random.uniform(1.2, 3.0)
        else:
            resolution_hours = target_hours * random.uniform(0.2, 0.95)

        resolved_at = created + timedelta(hours=resolution_hours)

        # Some tickets still open
        if random.random() < 0.08:
            status = random.choice(["open", "escalated"])
            resolved_at = None
            satisfaction_score = ""
        else:
            status = "resolved"
            # CSAT: churned accounts tend to rate lower
            if acct["status"] == "churned":
                satisfaction_score = round(random.uniform(1.0, 4.0), 1)
            else:
                satisfaction_score = round(random.uniform(2.5, 5.0), 1)

        tickets.append({
            "ticket_id": f"TKT-{tkt_counter:05d}",
            "account_id": acct["account_id"],
            "created_at": created.isoformat(),
            "resolved_at": resolved_at.isoformat() if resolved_at else "",
            "priority": priority,
            "category": category,
            "status": status,
            "satisfaction_score": satisfaction_score,
        })

with open("seeds/raw_support_tickets.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=tickets[0].keys())
    writer.writeheader()
    writer.writerows(tickets)

print(f"  -> {len(tickets)} support tickets")

# Summary
print("\n✅ All seed files generated in seeds/ folder:")
print(f"   raw_accounts.csv         ({len(accounts)} rows)")
print(f"   raw_subscriptions.csv    ({len(subscriptions)} rows)")
print(f"   raw_invoices.csv         ({len(invoices)} rows)")
print(f"   raw_usage_events.csv     ({len(usage_events)} rows)")
print(f"   raw_support_tickets.csv  ({len(tickets)} rows)")
print("\nCopy the seeds/ folder into your dbt project and run: dbt seed")
