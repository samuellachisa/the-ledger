#!/usr/bin/env python3
"""Check seed data for The Ledger Challenge."""
import psycopg2

def main():
    conn = psycopg2.connect('postgresql://postgres:12345@localhost:5432/apex_financial')
    cur = conn.cursor()
    
    # Check companies
    cur.execute('SELECT COUNT(*) FROM applicant_registry.companies')
    companies = cur.fetchone()[0]
    print(f'Companies: {companies}')
    
    # Check events
    cur.execute('SELECT COUNT(*) FROM events')
    events = cur.fetchone()[0]
    print(f'Events: {events}')
    
    # Find Montana companies
    cur.execute("SELECT company_id, name, state FROM applicant_registry.companies WHERE state = 'MT'")
    print('\nMontana companies:')
    rows = cur.fetchall()
    if rows:
        for row in rows:
            print(f'  {row[0]}: {row[1]}')
    else:
        print('  None found')
    
    # Check state distribution
    cur.execute('SELECT state, COUNT(*) FROM applicant_registry.companies GROUP BY state ORDER BY COUNT(*) DESC')
    print('\nTop 10 states:')
    for row in cur.fetchall()[:10]:
        print(f'  {row[0]}: {row[1]}')
    
    conn.close()
    print('\nData check complete!')

if __name__ == '__main__':
    main()
