from http.server import HTTPServer, BaseHTTPRequestHandler                                                                
import json                                                                                                               
                                                                                                                        
class H(BaseHTTPRequestHandler):                                                                                          
    def do_POST(self):                                                                                                    
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))                                           
        print(json.dumps(body, indent=2))                                                                                 
        self.send_response(200)                                                                                           
        self.end_headers()         
                                                                                                                        
HTTPServer(('', 9999), H).serve_forever()                                                                                 