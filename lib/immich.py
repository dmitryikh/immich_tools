from typing import List, Dict, Optional
import requests
from datetime import datetime, timezone
from colorama import Fore, Style, init

# Colorama init
init()

class ImmichAPI:
    """Class for working with Immich REST API"""
    
    def __init__(self, server_url: str, api_key: str):
        self.server_url = server_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            'x-api-key': api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def test_connection(self) -> bool:
        """Tests connection to Immich server"""
        try:
            response = requests.get(f"{self.server_url}/server/ping", headers=self.headers)
            return response.status_code == 200
        except Exception as e:
            print(f"{Fore.RED}Server connection error: {e}{Style.RESET_ALL}")
            return False

    def get_all_assets(self, asset_type: str = None, limit: int = None, album_id: str = None) -> List[str]:
        page = 1
        assets = []
        while True:
            body = {'page': page}
            if asset_type:
                body['type'] = asset_type
            if album_id:
                body['albumIds'] = [album_id]
            response = requests.post(f"{self.server_url}/search/metadata", headers=self.headers, json=body)
            if response.status_code != 200:
                raise ValueError(f"/search/metadata error: {response.status_code} - {response.text}")
            r = response.json()
            for asset in r['assets']['items']:
                if asset.get('isTrashed', False):
                    continue
                assets.append(asset['id'])
                if limit and len(assets) >= limit:
                    return assets

            if len(assets) % 1000 == 0:
                print(f'{len(assets)} asset ids fetched...')
            next_page = r['assets'].get('nextPage')
            if not next_page:
                break
            page = int(next_page)
        return assets

    def get_all_assets_from_album(self, album_name: str, asset_type: str = None) -> List[str]:
        album = self.get_album(album_name)
        if not album:
            raise ValueError(f"Album '{album_name}' not found")
        return self.get_all_assets(asset_type=asset_type, limit=None, album_id=album['id'])

    def get_asset_metadata(self, asset_id: str) -> Optional[Dict]:
        response = requests.get(f"{self.server_url}/assets/{asset_id}", headers=self.headers)
        if response.status_code != 200:
            raise ValueError(f"/assets/{asset_id} error: {response.status_code} - {response.text}")
        return response.json()

    def get_albums(self) -> List[Dict]:
        """Returns list of all albums"""
        response = requests.get(f"{self.server_url}/albums", headers=self.headers)
        if response.status_code != 200:
            raise ValueError(f"/albums error: {response.status_code} - {response.text}")
        return response.json()

    def get_album(self, name: str) -> Optional[Dict]:
        albums = self.get_albums()
        for album in albums:
            if album.get('albumName') == name:
                return album
        return None

    def update_asset_date(self, asset_id: str, new_date: datetime) -> bool:
        """Updates the date of an asset"""
        # Format: 2025-10-19T19:07:39+00:00
        if new_date.tzinfo is None:
            new_date = new_date.replace(tzinfo=timezone.utc)
        
        formatted_date = new_date.isoformat()
        
        body = {
            "dateTimeOriginal": formatted_date,
            "ids": [asset_id]
        }
        response = requests.put(f"{self.server_url}/assets/{asset_id}", 
                                headers=self.headers, 
                                json=body)
        if response.status_code != 200:
            raise ValueError(f"/assets/{asset_id} PUT error: {response.status_code} - {response.text}")
        return True

    def create_album(self, name: str, description: str = "", asset_ids: List[str] = []) -> Optional[str]:
        """Create a new album"""
        album_data = {
            "albumName": name,
            "description": description,
            "assetIds": asset_ids
        }
        response = requests.post(f"{self.server_url}/albums", 
                                headers=self.headers, 
                                json=album_data)
        if response.status_code != 201:
            raise ValueError(f"/album error: {response.status_code} - {response.text}")
        return response.json().get('id')
    