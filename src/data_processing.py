import json
import os
import pprint
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
            chunks = self.process_file(file_path)
            all_chunks.extend(chunks)
        return all_chunks
    
    def flatten_json_to_chunks(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        # convert nested JSON to flattened chunks for indexing
        chunks = []
        crop = json_data['crop']
        disease = json_data['disease']

        # overview chunk
        chunks.append({
            'text': f"Crop {crop}. Disease: {disease['name']}. Category: {disease['category']}. Severity: {disease['severity']}. {disease['severity_index']}.",
            'metadata': {
                'crop': crop,
                'disease': disease['name'],
                'section': 'overview',
                'category': disease['category'],
                'severity': disease['severity']
            }
        })

        # seasonal patterns
        seasonal = disease['seasonal_patterns']
        chunks.append({
            'text': f"Crop: {crop}. Disease: {disease['name']}. Peak occurrence: {seasonal['peak_occurrence']}. Favorable conditions: Temperature {seasonal['favorable_weather_conditions']['temperature']}, {seasonal['favorable_weather_conditions']['soil_moisture']} soil moisture, {seasonal['favorable_weather_conditions']['drainage']}.",
            'metadata': {
                'crop': crop,
                'disease': disease['name'],
                'section': 'seasonal_patterns',
                'peak_occurrence': seasonal['peak_occurrence']
            }
        })

        # environmental factors
        env = disease['environmental_factors']
        chunks.append({
            'text': f"Crop: {crop}. Disease: {disease['name']}. Environmental factors: {env['temperature_humidity']}. Soil conditions: {env['soil_conditions']}.",
            'metadata': {
                'crop': crop,
                'disease': disease['name'],
                'section': 'environmental_factors'
            }
        })

        # affected regions
        for region, districts in disease['affected_regions'].items():
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease['name']}. Affected region: {region}, Districts: {', '.join(districts)}. {disease['region_specific_information'].get(region, '')}",
                'metadata': {
                    'crop': crop,
                    'disease': disease['name'],
                    'section': 'affected_regions',
                    'region': region,
                    'districts': districts
                }
            })

        # symptoms by stage
        for stage, symptoms in disease['symptoms'].items():
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease['name']}. {stage.replace('_', ' ').title()} symptoms: {'. '.join(symptoms)}.",
                'metadata': {
                    'crop': crop,
                    'disease': disease['name'],
                    'section': 'symptoms',
                    'stage': stage
                }
            })


        # prevention methods
        for method, details in disease['prevention_methods'].items():
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease['name']}. Prevention method - {method.replace('_', ' ')}: {'. '.join(details)}.",
                'metadata': {
                    'crop': crop,
                    'disease': disease['name'],
                    'section': 'prevention',
                    'method': method
                }
            })

        # treatment
        for approach, methods in disease['treatment'].items():
            chunks.append({
                'text': f"Crop: {crop}. Disease: {disease['name']}. Treatment - {approach.replace('_', ' ')}: {'. '.join(methods)}.",
                'metadata': {
                    'crop': crop,
                    'disease': disease['name'],
                    'section': 'treatment',
                    'approach': approach
                }
            })
    
        return chunks

pp = pprint.PrettyPrinter(indent=2)

if __name__ == "__main__":
    processor = DataProcessor(data_dir="../data")
    files = processor.list_data_files()

    print(f"Found {len(files)} files.")

    for file_path in files:
        print(f"\nProcessing: {file_path}")
        chunks = processor.process_files(file_path)
        print(f"Generated {len(chunks)} chunks:")
        for chunk in chunks:
            pp.pprint(chunk)

