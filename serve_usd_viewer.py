#!/usr/bin/env python3
"""
Simple HTTP server to serve the USD viewer locally.
"""

import http.server
import socketserver
import webbrowser
import os
import sys

PORT = 8080

class CORSHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()

def serve_files():
    """Start a local HTTP server to serve the USD viewer."""
    
    # Change to the current directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Create server
    with socketserver.TCPServer(("", PORT), CORSHTTPRequestHandler) as httpd:
        print(f"Starting HTTP server on port {PORT}")
        print(f"Open your browser and go to: http://localhost:{PORT}/simple_usd_viewer.html")
        print("Press Ctrl+C to stop the server")
        
        # Try to open browser automatically
        try:
            webbrowser.open(f'http://localhost:{PORT}/simple_usd_viewer.html')
        except:
            pass
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
            sys.exit(0)

if __name__ == "__main__":
    serve_files()