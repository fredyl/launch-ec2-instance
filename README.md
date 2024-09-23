[Lytx Events API Instructions-2020aug04.pdf](https://github.com/user-attachments/files/17080579/Lytx.Events.API.Instructions-2020aug04.pdf)
[API - Developers Guide - v1.7 PUBLIC.pdf](https://github.com/user-attachments/files/17097947/API.-.Developers.Guide.-.v1.7.PUBLIC.pdf)


import requests

url = "https://lytx-api.prod5.ph.lytx.com/video/events"
secret = "t6nu2xUgxpYyA3OUbzpxO6atDQYj1Lxy"

headers = {
    'Authorization': f'Bearer {secret}'  # Add a space between 'Bearer' and the token
}
response = requests.get(url, headers=headers)
if response.status_code == 200:
    print("Success:", response.json())
else:
    raise Exception("Failed:", response.status_code, response.text)
    
Exception: ('Failed:', 401, '{![image](https://github.com/user-attachments/assets/087a2349-e119-449d-be7c-3cad3c94de20)
"message":"Unauthorized"}')<img width="680" alt="Screenshot 2024-09-23 114614" src="https://github.com/user-attachments/assets/9fca2121-c3ca-4411-90b7-d52c15c04e7f">
