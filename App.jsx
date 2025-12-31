/**
 * Inventory Barcode Scanning Application
 * Main App Component with Splash Screen
 *
 * @format
 */

import React, { useState } from 'react';
import { StatusBar, StyleSheet, useColorScheme, View, Text } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';
import SplashScreen from './src/screens/SplashScreen';

// Import with error handling
let BarcodeScanScreen;
let WeightEntryScreen;
try {
  BarcodeScanScreen = require('./src/screens/BarcodeScanScreen').default;
  WeightEntryScreen = require('./src/screens/WeightEntryScreen').default;
} catch (error) {
  console.error('Error loading screens:', error);
  BarcodeScanScreen = () => (
    <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center' }}>
      <Text>Error loading scanner. Please rebuild the app.</Text>
    </View>
  );
  WeightEntryScreen = () => (
    <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center' }}>
      <Text>Error loading weight screen. Please rebuild the app.</Text>
    </View>
  );
}

function App() {
  const [showSplash, setShowSplash] = useState(true);
  const [currentScreen, setCurrentScreen] = useState('splash');
  const [scannedBarcode, setScannedBarcode] = useState(null);
  const isDarkMode = useColorScheme() === 'dark';

  const handleSplashFinish = () => {
    setShowSplash(false);
    setCurrentScreen('scan');
  };

  const handleScanSuccess = (barcodeData) => {
    // Save scanned barcode and go to weight entry screen
    setScannedBarcode(barcodeData);
    setCurrentScreen('weight');
  };

  const handleBackToScan = () => {
    setCurrentScreen('scan');
  };

  const handleWeightSubmit = ({ barcode, weight, success, data }) => {
    // Handle submitted weight response
    if (success) {
      console.log('Weight submitted successfully:', {
        barcode,
        weight,
        cycleData: data?.cycle,
        routeStopData: data?.route_stop,
      });
      // After successful submission, go back to scan for the next item
      setCurrentScreen('scan');
      setScannedBarcode(null); // Clear scanned barcode
    } else {
      console.log('Weight submission failed:', { barcode, weight });
      // Stay on weight screen if submission failed (user can retry)
    }
  };

  return (
    <SafeAreaProvider>
      <StatusBar barStyle={isDarkMode ? 'light-content' : 'dark-content'} />
      {showSplash ? (
        <SplashScreen onFinish={handleSplashFinish} duration={3000} />
      ) : currentScreen === 'scan' ? (
        <BarcodeScanScreen
          onScanSuccess={handleScanSuccess}
          onBack={handleBackToScan}
        />
      ) : currentScreen === 'weight' && scannedBarcode ? (
        <WeightEntryScreen
          barcode={scannedBarcode}
          onSubmit={handleWeightSubmit}
          onBack={handleBackToScan}
        />
      ) : null}
    </SafeAreaProvider>
  );
}

export default App;
