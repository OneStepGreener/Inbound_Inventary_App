/**
 * WeightEntryScreen Component
 * Screen shown after a successful barcode scan where user can enter weight
 * White & blue theme to match the rest of the app
 *
 * @param {string} barcode - The scanned barcode value
 * @param {Function} onSubmit - Callback when weight is submitted (receives { barcode, weight })
 * @param {Function} onBack - Callback to go back to scanner without submitting
 */

import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TextInput,
  TouchableOpacity,
  Keyboard,
  StatusBar,
  Platform,
} from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

const WeightEntryScreen = ({ barcode, onSubmit, onBack }) => {
  const insets = useSafeAreaInsets();
  const [weight, setWeight] = useState('');
  const [isFocused, setIsFocused] = useState(false);

  const handleSubmit = () => {
    const trimmed = weight.trim();
    if (!trimmed) {
      return;
    }

    Keyboard.dismiss();
    if (onSubmit) {
      onSubmit({ barcode, weight: trimmed });
    }
  };

  return (
    <View style={styles.container}>
      <StatusBar
        barStyle="light-content"
        backgroundColor="#2196F3"
        translucent={Platform.OS === 'android'}
      />

      {/* Header */}
      <View style={[styles.header, { paddingTop: insets.top + 10 }]}> 
        <TouchableOpacity
          style={styles.backButton}
          onPress={onBack}
          activeOpacity={0.7}
        >
          <Text style={styles.backButtonText}>← Back</Text>
        </TouchableOpacity>
        <View style={styles.titleContainer}>
          <Text style={styles.title}>Enter Weight</Text>
          <Text style={styles.subtitle}>Barcode: {barcode}</Text>
        </View>
      </View>

      {/* Content */}
      <View style={[styles.content, { paddingBottom: insets.bottom + 20 }]}> 
        <View style={styles.card}>
          <Text style={styles.label}>Weight</Text>
          <TextInput
            style={[styles.input, isFocused && styles.inputFocused]}
            value={weight}
            onChangeText={setWeight}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="Enter weight"
            placeholderTextColor="#999"
            keyboardType="numeric"
            returnKeyType="done"
            onSubmitEditing={handleSubmit}
          />

          <TouchableOpacity
            style={[styles.submitButton, !weight.trim() && styles.submitButtonDisabled]}
            onPress={handleSubmit}
            activeOpacity={0.8}
            disabled={!weight.trim()}
          >
            <Text style={styles.submitButtonText}>Submit Weight</Text>
          </TouchableOpacity>
        </View>
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#FFFFFF',
  },
  header: {
    backgroundColor: '#2196F3',
    paddingHorizontal: 20,
    paddingBottom: 20,
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    zIndex: 10,
    shadowColor: '#000',
    shadowOffset: {
      width: 0,
      height: 2,
    },
    shadowOpacity: 0.25,
    shadowRadius: 3.84,
    elevation: 5,
  },
  backButton: {
    position: 'absolute',
    top: 0,
    left: 20,
    zIndex: 11,
  },
  titleContainer: {
    alignItems: 'center',
    marginTop: 10,
  },
  backButtonText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
  title: {
    fontSize: 28,
    fontWeight: 'bold',
    color: '#FFFFFF',
    marginBottom: 4,
    textAlign: 'center',
  },
  subtitle: {
    fontSize: 14,
    color: '#E3F2FD',
    textAlign: 'center',
  },
  content: {
    flex: 1,
    marginTop: 120,
    paddingHorizontal: 20,
    backgroundColor: '#F5F5F5',
  },
  card: {
    marginTop: 40,
    backgroundColor: '#FFFFFF',
    borderRadius: 16,
    padding: 20,
    shadowColor: '#000',
    shadowOffset: {
      width: 0,
      height: 2,
    },
    shadowOpacity: 0.1,
    shadowRadius: 3.84,
    elevation: 4,
  },
  label: {
    fontSize: 16,
    fontWeight: '600',
    color: '#333',
    marginBottom: 10,
  },
  input: {
    backgroundColor: '#F5F5F5',
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
    fontSize: 16,
    color: '#333',
    borderWidth: 2,
    borderColor: '#E0E0E0',
    marginBottom: 20,
  },
  inputFocused: {
    borderColor: '#2196F3',
    backgroundColor: '#FFFFFF',
  },
  submitButton: {
    backgroundColor: '#2196F3',
    borderRadius: 25,
    paddingVertical: 14,
    alignItems: 'center',
    shadowColor: '#2196F3',
    shadowOffset: {
      width: 0,
      height: 4,
    },
    shadowOpacity: 0.3,
    shadowRadius: 4.65,
    elevation: 8,
  },
  submitButtonDisabled: {
    backgroundColor: '#CCCCCC',
    shadowOpacity: 0,
    elevation: 0,
  },
  submitButtonText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
});

export default WeightEntryScreen;
