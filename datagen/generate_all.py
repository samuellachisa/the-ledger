"""
Main entry point for Apex Financial Services data generation.
Generates companies, financial documents, and exact 1,847 subset of events.
Coordinates database insertion with ON CONFLICT DO NOTHING.
"""

import argparse
import sys
import os
import json
import psycopg2
from psycopg2.extras import Json
from pathlib import Path
from uuid import UUID

from company_generator import generate_companies, to_database_records
from excel_generator import ExcelGenerator
from pdf_generator import PDFGenerator
from event_simulator import generate_events
from schema_validator import SchemaValidator

def setup_db(db_url):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Ensure applicant_registry exists
        cur.execute("CREATE SCHEMA IF NOT EXISTS applicant_registry;")
        
        # Create Tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS applicant_registry.companies (
                company_id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                industry TEXT NOT NULL,
                incorporation_date TIMESTAMP WITH TIME ZONE NOT NULL,
                employee_count INTEGER NOT NULL,
                annual_revenue NUMERIC(15, 2) NOT NULL,
                profile_type TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                zip_code TEXT NOT NULL,
                phone TEXT NOT NULL,
                email TEXT NOT NULL,
                tax_id TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS applicant_registry.financial_history (
                history_id UUID PRIMARY KEY,
                company_id UUID NOT NULL REFERENCES applicant_registry.companies(company_id),
                year INTEGER NOT NULL,
                revenue NUMERIC(15, 2) NOT NULL,
                net_income NUMERIC(15, 2) NOT NULL,
                total_assets NUMERIC(15, 2) NOT NULL,
                total_liabilities NUMERIC(15, 2) NOT NULL,
                ebitda NUMERIC(15, 2) NOT NULL,
                operating_cash_flow NUMERIC(15, 2) NOT NULL,
                UNIQUE (company_id, year)
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS applicant_registry.compliance_flags (
                flag_id UUID PRIMARY KEY,
                company_id UUID NOT NULL REFERENCES applicant_registry.companies(company_id),
                flag_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                description TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                resolved_at TIMESTAMP WITH TIME ZONE
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS applicant_registry.loan_relationships (
                relationship_id UUID PRIMARY KEY,
                company_id UUID NOT NULL REFERENCES applicant_registry.companies(company_id),
                lender_name TEXT NOT NULL,
                loan_amount NUMERIC(15, 2) NOT NULL,
                interest_rate NUMERIC(6, 4) NOT NULL,
                start_date TIMESTAMP WITH TIME ZONE NOT NULL,
                maturity_date TIMESTAMP WITH TIME ZONE NOT NULL,
                status TEXT NOT NULL
            );
        """)
        
    return conn

def insert_companies(conn, db_records):
    """Insert companies and relations idempotently."""
    with conn.cursor() as cur:
        # Companies
        for c in db_records["companies"]:
            cur.execute("""
                INSERT INTO applicant_registry.companies 
                (company_id, name, industry, incorporation_date, employee_count, annual_revenue, 
                 profile_type, risk_level, address, city, state, zip_code, phone, email, tax_id, 
                 created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_id) DO NOTHING;
            """, (
                str(c["company_id"]), c["name"], c["industry"], c["incorporation_date"], 
                c["employee_count"], c["annual_revenue"], c["profile_type"], 
                c["risk_level"], c["address"], c["city"], c["state"], c["zip_code"], 
                c["phone"], c["email"], c["tax_id"], c["created_at"], c["updated_at"]
            ))
            
        # Financial History
        for f in db_records["financial_history"]:
            cur.execute("""
                INSERT INTO applicant_registry.financial_history
                (history_id, company_id, year, revenue, net_income, total_assets, total_liabilities, ebitda, operating_cash_flow)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (history_id) DO NOTHING;
            """, (str(f["history_id"]), str(f["company_id"]), f["year"], f["revenue"], f["net_income"], 
                  f["total_assets"], f["total_liabilities"], f["ebitda"], f["operating_cash_flow"]))

        # Compliance Flags
        for cf in db_records["compliance_flags"]:
            cur.execute("""
                INSERT INTO applicant_registry.compliance_flags
                (flag_id, company_id, flag_type, severity, description, created_at, resolved_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (flag_id) DO NOTHING;
            """, (str(cf["flag_id"]), str(cf["company_id"]), cf["flag_type"], cf["severity"], 
                  cf["description"], cf["created_at"], cf["resolved_at"]))

        # Loan Relationships
        for lr in db_records["loan_relationships"]:
            cur.execute("""
                INSERT INTO applicant_registry.loan_relationships
                (relationship_id, company_id, lender_name, loan_amount, interest_rate, start_date, maturity_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (relationship_id) DO NOTHING;
            """, (str(lr["relationship_id"]), str(lr["company_id"]), lr["lender_name"], lr["loan_amount"], 
                  lr["interest_rate"], lr["start_date"], lr["maturity_date"], lr["status"]))

def get_event_streams_uuid(app_id_str):
    """Generate deterministically unique UUIDs for streams based on aggregate ID strings."""
    import uuid
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, app_id_str))

def insert_events(conn, events):
    """Insert events and streams idempotently."""
    with conn.cursor() as cur:
        # Group by stream
        streams = {}
        for ev in events:
            agg_type = ev["aggregate_type"]
            agg_id = ev["aggregate_id"]
            if agg_id not in streams:
                # generate surrogate UUID stream identifier
                streams[agg_id] = {
                    "stream_id": get_event_streams_uuid(agg_id),
                    "aggregate_type": agg_type,
                    "aggregate_id": agg_id,
                    "current_version": 0
                }
            streams[agg_id]["current_version"] = max(streams[agg_id]["current_version"], ev["stream_position"])
            
        # Insert streams
        for s in streams.values():
            if not is_valid_uuid(s["aggregate_id"]):
                # If agg_id is not a UUID (like "APEX-0001"), convert it to UUID for DB storage conceptually
                # Event schema expects uuid for aggregate_id
                agg_uuid = get_event_streams_uuid(s["aggregate_id"])
            else:
                agg_uuid = s["aggregate_id"]
                
            cur.execute("""
                INSERT INTO event_streams
                (stream_id, aggregate_type, aggregate_id, current_version)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET current_version = GREATEST(event_streams.current_version, EXCLUDED.current_version);
            """, (s["stream_id"], s["aggregate_type"], agg_uuid, s["current_version"]))

        # Insert events
        for ev in events:
            stream_id = get_event_streams_uuid(ev["aggregate_id"])
            agg_uuid = get_event_streams_uuid(ev["aggregate_id"]) if not is_valid_uuid(ev["aggregate_id"]) else ev["aggregate_id"]
            
            cur.execute("""
                INSERT INTO events
                (event_id, stream_id, aggregate_type, aggregate_id, event_type, schema_version, stream_position, payload, metadata, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stream_id, stream_position) DO NOTHING;
            """, (
                ev["event_id"], stream_id, ev["aggregate_type"], agg_uuid, ev["event_type"],
                ev["schema_version"], ev["stream_position"], Json(ev["payload"]), Json(ev.get("metadata", {})), ev["recorded_at"]
            ))

def is_valid_uuid(val):
    import uuid
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--applicants", type=int, default=80)
    parser.add_argument("--db-url", type=str, required=True)
    parser.add_argument("--docs-dir", type=str, default="./documents")
    parser.add_argument("--output-dir", type=str, default="./data")
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--validate-only", action="store_true")
    
    args = parser.parse_args()
    
    # Generate 80 companies
    companies = generate_companies(args.applicants, random_seed=args.random_seed)
    
    # Generate 1847 Events
    events = generate_events(companies, random_seed=args.random_seed)
    
    # Validate Events
    validator = SchemaValidator()
    result = validator.validate_events(events)
    
    if args.validate_only:
        if result.valid:
            print("0 errors")
            sys.exit(0)
        else:
            print(f"{len(result.errors)} errors found:")
            for e in result.errors[:10]:
                print(e)
            sys.exit(1)
            
    if not result.valid:
        print(f"Validation failed with {len(result.errors)} errors.")
        sys.exit(1)
        
    print(f"Validation successful. {len(events)} events generated.")
    
    # Generate Docs
    pdf_gen = PDFGenerator(random_seed=args.random_seed)
    excel_gen = ExcelGenerator(random_seed=args.random_seed)
    
    # Generate applications list for proposals
    applications = [{"application_id": f"APEX-{i+1:04d}", "company_id": companies[i % len(companies)].company_id, "loan_amount": 5000000.0} for i in range(29)]
    
    pdf_gen.generate_all_pdfs(companies, applications, args.docs_dir)
    excel_gen.generate_all_excel(companies, args.docs_dir)
    
    print(f"Generated documents in {args.docs_dir}")
    
    # Insert to DB
    print("Inserting data to PostgreSQL...")
    conn = setup_db(args.db_url)
    db_records = to_database_records(companies)
    insert_companies(conn, db_records)
    insert_events(conn, events)
    conn.close()
    
    print("Done!")
    sys.exit(0)

if __name__ == "__main__":
    main()
