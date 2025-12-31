/**
 * Frontend-Backend Connection Test Script
 * Run this in Node.js or browser console to test connectivity
 */

// Configuration - Update these to match your setup
const BACKEND_URL = 'http://192.168.5.8:5000/aiml/corporatewebsite';
// Alternative URLs to try:
// const BACKEND_URL = 'http://localhost:5000/aiml/corporatewebsite';
// const BACKEND_URL = 'http://10.0.2.2:5000/aiml/corporatewebsite'; // Android emulator

async function testConnection() {
    console.log('='.repeat(60));
    console.log('FRONTEND-BACKEND CONNECTION TEST');
    console.log('='.repeat(60));
    console.log(`Testing connection to: ${BACKEND_URL}`);
    console.log('');

    // Test 1: Basic connectivity
    console.log('Test 1: Testing basic connectivity...');
    try {
        const response = await fetch(`${BACKEND_URL}/test/connection`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        });

        if (response.ok) {
            const data = await response.json();
            console.log('✅ Connection successful!');
            console.log('Response:', JSON.stringify(data, null, 2));
        } else {
            console.log(`❌ Connection failed with status: ${response.status}`);
            const text = await response.text();
            console.log('Response:', text);
        }
    } catch (error) {
        console.log('❌ Connection error:', error.message);
        console.log('Error details:', error);
    }

    console.log('');

    // Test 2: Database connection
    console.log('Test 2: Testing database connection...');
    try {
        const response = await fetch(`${BACKEND_URL}/test/database`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        });

        if (response.ok) {
            const data = await response.json();
            console.log('✅ Database connection test successful!');
            console.log('Database Info:', JSON.stringify(data, null, 2));
        } else {
            console.log(`❌ Database test failed with status: ${response.status}`);
            const text = await response.text();
            console.log('Response:', text);
        }
    } catch (error) {
        console.log('❌ Database test error:', error.message);
    }

    console.log('');

    // Test 3: CORS test
    console.log('Test 3: Testing CORS configuration...');
    try {
        const response = await fetch(`${BACKEND_URL}/test/connection`, {
            method: 'OPTIONS',
            headers: {
                'Origin': 'http://localhost:3000',
                'Access-Control-Request-Method': 'GET',
            },
        });
        console.log('CORS Preflight Status:', response.status);
        console.log('CORS Headers:', {
            'Access-Control-Allow-Origin': response.headers.get('Access-Control-Allow-Origin'),
            'Access-Control-Allow-Methods': response.headers.get('Access-Control-Allow-Methods'),
        });
    } catch (error) {
        console.log('❌ CORS test error:', error.message);
    }

    console.log('');
    console.log('='.repeat(60));
    console.log('TEST COMPLETE');
    console.log('='.repeat(60));
}

// Run the test
testConnection();
