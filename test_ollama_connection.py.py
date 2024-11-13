import requests
import time


def test_connection():
    url = "http://127.0.0.1:11435/api/generate"
    payload = {"model": "mixtral", "prompt": "hello", "stream": False}

    print(f"Testing connection to {url}")
    print("Payload:", payload)

    try:
        # Increase timeout and add headers
        response = requests.post(
            url, json=payload, timeout=30, headers={"Content-Type": "application/json"}
        )

        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")

        if response.status_code == 200:
            print("Success!")
            print("Response:", response.json())
            return True

        print("Error Response:", response.text)
        return False

    except requests.exceptions.Timeout:
        print("Connection timed out. Check if Ollama is responding.")
    except requests.exceptions.ConnectionError:
        print("Connection failed. Check if Ollama is running and accessible.")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(f"Error type: {type(e)}")

    return False


if __name__ == "__main__":
    test_connection()
