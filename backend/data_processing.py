import json
import os
# import pprint
from typing import List, Dict, Any, Optional

class DataProcessor:
    def __init__(self, data_dir: str = "data/raw"):
        # initialise data procressor with data directory
        self.data_dir = data_dir
    
    def load_file(self, file_path: str) -> Dict[str, Any]:
        # loads a single json file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    def list_data_files(self, file_extension: str = ".json") -> List[str]:
        # list all data files in the data directory with the specified exension
        file_paths = []
        for root, _, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith(file_extension):
                    file_paths.append(os.path.join(root, file))
        return file_paths
    
    def process_files(self, file_path: str) -> List[Dict[str, Any]]:
        # process a single file into chunks for suitable embedding
        data = self.load_file(file_path)
        chunks = self.flatten_json_to_chunks(data)
        return chunks
    
    def process_all_files(self) -> List[Dict[str, Any]]:
        # process all files in the data directory
        all_chunks = []
        file_paths = self.list_data_files()

        for file_path in file_paths:
            chunks = self.process_files(file_path)
            all_chunks.extend(chunks)
        return all_chunks
    
    def flatten_json_to_chunks(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        # convert nested JSON to flattened chunks for indexing
        chunks = []
        crop = json_data.get('crop', 'not available')
        disease = json_data.get('disease', {})

        # Get disease details with defaults for missing fields
        disease_name = disease.get('name', 'not available')
        disease_category = disease.get('category', 'not available')
        disease_severity = disease.get('severity', 'not available')
        disease_severity_index = disease.get('severity_index', 'not available')

        # overview chunk
        chunks.append({
            'text': f"Crop {crop}. Disease: {disease_name}. Category: {disease_category}. Severity: {disease_severity}. {disease_severity_index}.",
            'metadata': {
                'crop': crop,
                'disease': disease_name,
                'section': 'overview',
                'category': disease_category,
                'severity': disease_severity
            }
        })

        # seasonal patterns
        seasonal = disease.get('seasonal_patterns', {})
        peak_occurrence = seasonal.get('peak_occurrence', 'not available')
        
        # Handle nested weather conditions with defaults
        favorable_conditions = seasonal.get('favorable_weather_conditions', {})
        temperature = favorable_conditions.get('temperature', 'not available')
        soil_moisture = favorable_conditions.get('soil_moisture', 'not available')
        drainage = favorable_conditions.get('drainage', 'not available')
        
        chunks.append({
            'text': f"Crop: {crop}. Disease: {disease_name}. Peak occurrence: {peak_occurrence}. Favorable conditions: Temperature {temperature}, {soil_moisture} soil moisture, {drainage}.",
            'metadata': {
                'crop': crop,
                'disease': disease_name,
                'section': 'seasonal_patterns',
                'peak_occurrence': peak_occurrence
            }
        })

        # environmental factors
        env = disease.get('environmental_factors', {})
        temp_humidity = env.get('temperature_humidity', 'not available')
        soil_conditions = env.get('soil_conditions', 'not available')
        
        chunks.append({
            'text': f"Crop: {crop}. Disease: {disease_name}. Environmental factors: {temp_humidity}. Soil conditions: {soil_conditions}.",
            'metadata': {
                'crop': crop,
                'disease': disease_name,
                'section': 'environmental_factors'
            }
        })

        # affected regions
        regions = disease.get('affected_regions', {})
        region_info = disease.get('region_specific_information', {})
        
        for region, districts in regions.items():
            # Handle both dictionary and string cases for region_info
            if isinstance(region_info, dict):
                region_specific_info = region_info.get(region, 'not available')
            else:
                # If region_info is a string (global info for all regions)
                region_specific_info = region_info if region_info else 'not available'
                
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease_name}. Affected region: {region}, Districts: {', '.join(districts)}. {region_specific_info}",
                'metadata': {
                    'crop': crop,
                    'disease': disease_name,
                    'section': 'affected_regions',
                    'region': region,
                    'districts': districts
                }
            })

        # symptoms by stage
        symptoms = disease.get('symptoms', {})
        for stage, symptom_list in symptoms.items():
            if not symptom_list:  # If symptom list is empty
                symptom_list = ['not available']
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease_name}. {stage.replace('_', ' ').title()} symptoms: {'. '.join(symptom_list)}.",
                'metadata': {
                    'crop': crop,
                    'disease': disease_name,
                    'section': 'symptoms',
                    'stage': stage
                }
            })

        # prevention methods
        prevention = disease.get('prevention_methods', {})
        for method, details in prevention.items():
            if not details:  # If details list is empty
                details = ['not available']
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease_name}. Prevention method - {method.replace('_', ' ')}: {'. '.join(details)}.",
                'metadata': {
                    'crop': crop,
                    'disease': disease_name,
                    'section': 'prevention',
                    'method': method
                }
            })

        # treatment
        treatment = disease.get('treatment', {})
        for approach, methods in treatment.items():
            if not methods:  # If methods list is empty
                methods = ['not available']
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease_name}. Treatment - {approach.replace('_', ' ')}: {'. '.join(methods)}.",
                'metadata': {
                    'crop': crop,
                    'disease': disease_name,
                    'section': 'treatment',
                    'approach': approach
                }
            })
    
        return chunks

# pp = pprint.PrettyPrinter(indent=2)

# if __name__ == "__main__":
#     processor = DataProcessor(data_dir="../data")
#     files = processor.list_data_files()

#     print(f"Found {len(files)} files.")

#     for file_path in files:
#         print(f"\nProcessing: {file_path}")
#         chunks = processor.process_files(file_path)
#         print(f"Generated {len(chunks)} chunks:")
#         for chunk in chunks:
#             pp.pprint(chunk)