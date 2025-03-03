import { fetchSupremacismData, fetchCountryAnalysis, runGdeltAnalysis, fetchRegionalSummary, fetchEventData, fetchNlpAnalysis } from '@/lib/gdeltApi';

/**
 * GDELT Service
 * Provides a layer between the React components and the API client
 * with type definitions and additional processing
 */

// Define types for SGM data based on your models
export interface CountryData {
  code: string;
  country: string;
  srsD?: number;     // Domestic Supremacism Risk Score
  srsI?: number;     // International Supremacism Risk Score
  sgm: number;       // Supremacism Global Metric
  gscs?: number;     // Global Supremacism Composite Score (legacy)
  latitude: number;
  longitude: number;
  sti?: number;      // Stability and Transition Index
  category?: string;
  description?: string;
  event_count?: number;
  avg_tone?: number;
  updated_at?: string;
}

export interface RegionalData {
  region: string;
  avg_sgm: number;
  countries: number;
  highest_country: string;
  highest_sgm: number;
  lowest_country: string;
  lowest_sgm: number;
}

export interface EventData {
  date: string;
  country: string;
  event_count: number;
  avg_tone: number;
  event_codes: string[];
  themes: string[];
}

export interface NlpAnalysisData {
  country: string;
  sentiment_score: number;
  top_themes: string[];
  related_countries: string[];
  entity_analysis: Record<string, number>;
}

/**
 * Get SGM data for all countries to display on the map
 * @returns Promise<CountryData[]> List of country data with SGM scores
 */
export const getSupremacismData = async (): Promise<CountryData[]> => {
  try {
    const data = await fetchSupremacismData();
    return data.map(country => ({
      ...country,
      // Set category based on SGM score if not provided
      category: country.category || calculateCategory(country.sgm || country.gscs || 0)
    }));
  } catch (error) {
    console.error('Error in getSupremacismData:', error);
    throw error;
  }
};

/**
 * Get detailed analysis for a specific country
 * @param countryCode ISO country code
 * @returns Promise<CountryData> Detailed country data
 */
export const getCountryAnalysis = async (countryCode: string): Promise<CountryData> => {
  try {
    const data = await fetchCountryAnalysis(countryCode);
    return {
      ...data,
      // Set category based on SGM score if not provided
      category: data.category || calculateCategory(data.sgm || data.gscs || 0)
    };
  } catch (error) {
    console.error(`Error in getCountryAnalysis for ${countryCode}:`, error);
    throw error;
  }
};

/**
 * Get regional summary data for the trends tab
 * @returns Promise<RegionalData[]> Regional summary data
 */
export const getRegionalSummary = async (): Promise<RegionalData[]> => {
  try {
    return await fetchRegionalSummary();
  } catch (error) {
    console.error('Error in getRegionalSummary:', error);
    throw error;
  }
};

/**
 * Trigger a new GDELT analysis run
 * @returns Promise<{jobId: string, status: string}> Analysis job status
 */
export const triggerGdeltAnalysis = async (): Promise<{jobId: string, status: string}> => {
  try {
    return await runGdeltAnalysis();
  } catch (error) {
    console.error('Error in triggerGdeltAnalysis:', error);
    throw error;
  }
};

/**
 * Get GDELT event data for visualization
 * @returns Promise<EventData[]> GDELT event data
 */
export const getEventData = async (): Promise<EventData[]> => {
  try {
    return await fetchEventData();
  } catch (error) {
    console.error('Error in getEventData:', error);
    throw error;
  }
};

/**
 * Get NLP analysis results
 * @returns Promise<NlpAnalysisData[]> NLP analysis results
 */
export const getNlpAnalysis = async (): Promise<NlpAnalysisData[]> => {
  try {
    return await fetchNlpAnalysis();
  } catch (error) {
    console.error('Error in getNlpAnalysis:', error);
    throw error;
  }
};

/**
 * Helper function to calculate category based on SGM score
 * @param score SGM score
 * @returns Category string
 */
const calculateCategory = (score: number): string => {
  if (score <= 2) return "Non-Supremacist Governance";
  if (score <= 4) return "Mixed Governance";
  if (score <= 6) return "Soft Supremacism";
  if (score <= 8) return "Structural Supremacism";
  return "Extreme Supremacism";
};