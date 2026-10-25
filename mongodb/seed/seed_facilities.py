#!/usr/bin/env python3
"""
MongoDB Recycling Infrastructure Seeder
Populates MongoDB collection 'facilities' with realistic European e-waste recycling,
smelting, depollution, and metal recovery facilities matching FutuRaM country codes.
All sample/demo data is explicitly marked with is_sample_data: True.
"""

import os
import sys
import json
from pymongo import MongoClient

FACILITIES = [
    {
        "facility_id": "FAC-DEU-001",
        "name": "Aurubis Primary & Secondary Copper/Metal Smelter",
        "country_code": "DEU",
        "country_name": "Germany",
        "city": "Hamburg",
        "latitude": 53.5283,
        "longitude": 10.0215,
        "annual_capacity_mg": 280000.0,
        "facility_type": "Specialized Smelter & Refining",
        "processing_types": ["Pyrometallurgy", "Electrolytic Refining", "Precious Metal Extraction"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat2", "WEEE_Cat4a", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-DEU-002",
        "name": "Remondis Electro-Recycling Center Lünen",
        "country_code": "DEU",
        "country_name": "Germany",
        "city": "Lünen",
        "latitude": 51.6167,
        "longitude": 7.5167,
        "annual_capacity_mg": 95000.0,
        "facility_type": "Integrated WEEE Dismantling & Sorting",
        "processing_types": ["Mechanical Shredding", "Optical Sensor Sorting", "Depollution"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-DEU-003",
        "name": "TSR Recycling E-Waste Plant",
        "country_code": "DEU",
        "country_name": "Germany",
        "city": "Duisburg",
        "latitude": 51.4344,
        "longitude": 6.7623,
        "annual_capacity_mg": 120000.0,
        "facility_type": "Mechanical Separation & Shredding",
        "processing_types": ["Heavy Media Separation", "Magnetic Separation", "Eddy Current"],
        "accepted_categories": ["WEEE_Cat4a", "WEEE_Cat5"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-GBR-001",
        "name": "AO Recycling Cooling & Large Appliance Plant",
        "country_code": "GBR",
        "country_name": "United Kingdom",
        "city": "Telford",
        "latitude": 52.6784,
        "longitude": -2.4453,
        "annual_capacity_mg": 110000.0,
        "facility_type": "Cooling & Temperature Depollution",
        "processing_types": ["CFC/HCFC Degassing", "Hermetic Shredding", "Polyurethane Foam Recovery"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-GBR-002",
        "name": "EWRG Huddersfield IT & Telecom Processing Facility",
        "country_code": "GBR",
        "country_name": "United Kingdom",
        "city": "Huddersfield",
        "latitude": 53.6458,
        "longitude": -1.7850,
        "annual_capacity_mg": 45000.0,
        "facility_type": "Precious & Critical Metal Recovery",
        "processing_types": ["PCB Dismantling", "Hydrometallurgical Pre-treatment", "Circuit Board Milling"],
        "accepted_categories": ["WEEE_Cat2", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-FRA-001",
        "name": "Derichebourg Environnement Lyon WEEE Platform",
        "country_code": "FRA",
        "country_name": "France",
        "city": "Saint-Fons (Lyon)",
        "latitude": 45.7067,
        "longitude": 4.8569,
        "annual_capacity_mg": 85000.0,
        "facility_type": "Mechanical Separation & Shredding",
        "processing_types": ["Automated De-manufacturing", "Eddy Current", "Air Classification"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a", "WEEE_Cat5"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-FRA-002",
        "name": "Veolia Valopôle Tri & Traitement DEEE",
        "country_code": "FRA",
        "country_name": "France",
        "city": "Angers",
        "latitude": 47.4784,
        "longitude": -0.5632,
        "annual_capacity_mg": 65000.0,
        "facility_type": "Integrated WEEE Dismantling & Sorting",
        "processing_types": ["Manual Dismantling", "Display Depollution", "Optical Sorting"],
        "accepted_categories": ["WEEE_Cat2", "WEEE_Cat3", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-ITA-001",
        "name": "Relight WEEE High-Tech Recovery Plant",
        "country_code": "ITA",
        "country_name": "Italy",
        "city": "Rho (Milano)",
        "latitude": 45.5317,
        "longitude": 9.0408,
        "annual_capacity_mg": 70000.0,
        "facility_type": "Precious & Critical Metal Recovery",
        "processing_types": ["Fluorescent Powder Extraction", "Hydrometallurgy", "Rare Earth Recovery"],
        "accepted_categories": ["WEEE_Cat2", "WEEE_Cat3", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-ITA-002",
        "name": "Stena Recycling Taranto Processing Complex",
        "country_code": "ITA",
        "country_name": "Italy",
        "city": "Taranto",
        "latitude": 40.4644,
        "longitude": 17.2470,
        "annual_capacity_mg": 55000.0,
        "facility_type": "Mechanical Separation & Shredding",
        "processing_types": ["Shredding", "Magnetic Separation", "Depollution"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a", "WEEE_Cat5"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-ESP-001",
        "name": "Recyclia / Indumental E-Waste Recycling Hub",
        "country_code": "ESP",
        "country_name": "Spain",
        "city": "Erandio (Bilbao)",
        "latitude": 43.3039,
        "longitude": -2.9733,
        "annual_capacity_mg": 80000.0,
        "facility_type": "Specialized Smelter & Refining",
        "processing_types": ["Complex Scrap Refining", "Mechanical Fractionation", "Plastics Flotation"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat2", "WEEE_Cat4a", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-ESP-002",
        "name": "Electroreciclaje Levantino",
        "country_code": "ESP",
        "country_name": "Spain",
        "city": "Valencia",
        "latitude": 39.4699,
        "longitude": -0.3763,
        "annual_capacity_mg": 48000.0,
        "facility_type": "Cooling & Temperature Depollution",
        "processing_types": ["Coolant Recovery", "Polyurethane Compacting", "Metal Recovery"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-NLD-001",
        "name": "Coolrec (Renewi) Waalwijk High-Tech WEEE Center",
        "country_code": "NLD",
        "country_name": "Netherlands",
        "city": "Waalwijk",
        "latitude": 51.6869,
        "longitude": 5.0681,
        "annual_capacity_mg": 90000.0,
        "facility_type": "Integrated WEEE Dismantling & Sorting",
        "processing_types": ["Cryogenic Separation", "Electrostatic Polymer Sorting", "CFC Extraction"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a", "WEEE_Cat5"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-NLD-002",
        "name": "Sims Lifecycle Services Eindhoven",
        "country_code": "NLD",
        "country_name": "Netherlands",
        "city": "Eindhoven",
        "latitude": 51.4416,
        "longitude": 5.4697,
        "annual_capacity_mg": 38000.0,
        "facility_type": "Precious & Critical Metal Recovery",
        "processing_types": ["Data Sanitization", "Component Harvesting", "High-Grade PCB Recovery"],
        "accepted_categories": ["WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-BEL-001",
        "name": "Umicore Precious Metals Refining Complex",
        "country_code": "BEL",
        "country_name": "Belgium",
        "city": "Hoboken (Antwerp)",
        "latitude": 51.1772,
        "longitude": 4.3547,
        "annual_capacity_mg": 350000.0,
        "facility_type": "Specialized Smelter & Refining",
        "processing_types": ["Ultra-High Temperature Smelting", "28 Metal Recovery System", "Hydrometallurgical Refining"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat2", "WEEE_Cat4a", "WEEE_Cat4b", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-POL-001",
        "name": "Elemental Holding Zawiercie Smelter & E-Waste Center",
        "country_code": "POL",
        "country_name": "Poland",
        "city": "Zawiercie",
        "latitude": 50.4877,
        "longitude": 19.4168,
        "annual_capacity_mg": 60000.0,
        "facility_type": "Battery & Accumulator Recycling",
        "processing_types": ["Li-Ion Battery Black Mass Recovery", "PCB Refining", "Secondary Smelting"],
        "accepted_categories": ["WEEE_Cat4b", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-SWE-001",
        "name": "Boliden Rönnskär Smelter & Electronic Scrap Unit",
        "country_code": "SWE",
        "country_name": "Sweden",
        "city": "Skellefteå",
        "latitude": 64.6714,
        "longitude": 21.2583,
        "annual_capacity_mg": 120000.0,
        "facility_type": "Specialized Smelter & Refining",
        "processing_types": ["Kaldo Converter Smelting", "Copper Anode Casting", "Gold/Silver Recovery"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat2", "WEEE_Cat4a", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-AUT-001",
        "name": "Müller-Guttenbrunn Group Amstetten E-Waste Facility",
        "country_code": "AUT",
        "country_name": "Austria",
        "city": "Amstetten",
        "latitude": 48.1228,
        "longitude": 14.8722,
        "annual_capacity_mg": 52000.0,
        "facility_type": "Mechanical Separation & Shredding",
        "processing_types": ["Advanced Polymer Sorting", "Metals Separation", "Dense Media Separation"],
        "accepted_categories": ["WEEE_Cat1", "WEEE_Cat4a", "WEEE_Cat5"],
        "status": "Operational",
        "is_sample_data": True
    },
    {
        "facility_id": "FAC-CHE-001",
        "name": "Immark AG Regensdorf Circular E-Waste Facility",
        "country_code": "CHE",
        "country_name": "Switzerland",
        "city": "Regensdorf (Zürich)",
        "latitude": 47.4333,
        "longitude": 8.4667,
        "annual_capacity_mg": 42000.0,
        "facility_type": "Integrated WEEE Dismantling & Sorting",
        "processing_types": ["Automated Robotics Sorting", "Toner & Battery Safe Depollution", "Optical Sorting"],
        "accepted_categories": ["WEEE_Cat2", "WEEE_Cat4a", "WEEE_Cat5", "WEEE_Cat6"],
        "status": "Operational",
        "is_sample_data": True
    }
]

def seed_mongodb(mongo_uri="mongodb://localhost:27017"):
    print(f"Connecting to MongoDB at: {mongo_uri}")
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=4000)
        # Verify connection
        client.admin.command('ping')
        print("Connected to MongoDB successfully.")
        
        db = client["ewaste_db"]
        collection = db["facilities"]
        
        # Reset and seed collection
        collection.drop()
        result = collection.insert_many(FACILITIES)
        print(f"Inserted {len(result.inserted_ids)} facility documents into ewaste_db.facilities.")
        
        # Create indexes
        collection.create_index("facility_id", unique=True)
        collection.create_index("country_code")
        collection.create_index("facility_type")
        collection.create_index([("latitude", 1), ("longitude", 1)])
        print("Created indexes: facility_id (unique), country_code, facility_type, (latitude, longitude).")

        # Summary output
        print("\nSummary by Country:")
        pipeline = [
            {"$group": {"_id": "$country_name", "count": {"$sum": 1}, "total_capacity": {"$sum": "$annual_capacity_mg"}}},
            {"$sort": {"total_capacity": -1}}
        ]
        for row in collection.aggregate(pipeline):
            print(f"  {row['_id']:16}: {row['count']} facilities, {row['total_capacity']:,.0f} Mg/year capacity")

        client.close()
        print("\nMongoDB Seeding Complete.")
        return True
    except Exception as e:
        print(f"ERROR connecting to or seeding MongoDB: {e}")
        return False

if __name__ == "__main__":
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    seed_mongodb(uri)
