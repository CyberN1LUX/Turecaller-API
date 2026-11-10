// Common countries: iso2 -> { name, dial, len (typical national number length) }
export const COUNTRIES = {
  IN: { name: 'India', dial: '91', len: 10 },
  US: { name: 'United States', dial: '1', len: 10 },
  CA: { name: 'Canada', dial: '1', len: 10 },
  GB: { name: 'United Kingdom', dial: '44', len: 10 },
  AE: { name: 'UAE', dial: '971', len: 9 },
  SA: { name: 'Saudi Arabia', dial: '966', len: 9 },
  QA: { name: 'Qatar', dial: '974', len: 8 },
  KW: { name: 'Kuwait', dial: '965', len: 8 },
  OM: { name: 'Oman', dial: '968', len: 8 },
  BH: { name: 'Bahrain', dial: '973', len: 8 },
  PK: { name: 'Pakistan', dial: '92', len: 10 },
  BD: { name: 'Bangladesh', dial: '880', len: 10 },
  NP: { name: 'Nepal', dial: '977', len: 10 },
  LK: { name: 'Sri Lanka', dial: '94', len: 9 },
  MY: { name: 'Malaysia', dial: '60', len: 9 },
  SG: { name: 'Singapore', dial: '65', len: 8 },
  ID: { name: 'Indonesia', dial: '62', len: 10 },
  PH: { name: 'Philippines', dial: '63', len: 10 },
  TH: { name: 'Thailand', dial: '66', len: 9 },
  VN: { name: 'Vietnam', dial: '84', len: 9 },
  CN: { name: 'China', dial: '86', len: 11 },
  JP: { name: 'Japan', dial: '81', len: 10 },
  KR: { name: 'South Korea', dial: '82', len: 10 },
  HK: { name: 'Hong Kong', dial: '852', len: 8 },
  TW: { name: 'Taiwan', dial: '886', len: 9 },
  AU: { name: 'Australia', dial: '61', len: 9 },
  NZ: { name: 'New Zealand', dial: '64', len: 9 },
  DE: { name: 'Germany', dial: '49', len: 11 },
  FR: { name: 'France', dial: '33', len: 9 },
  IT: { name: 'Italy', dial: '39', len: 10 },
  ES: { name: 'Spain', dial: '34', len: 9 },
  PT: { name: 'Portugal', dial: '351', len: 9 },
  NL: { name: 'Netherlands', dial: '31', len: 9 },
  BE: { name: 'Belgium', dial: '32', len: 9 },
  CH: { name: 'Switzerland', dial: '41', len: 9 },
  AT: { name: 'Austria', dial: '43', len: 10 },
  SE: { name: 'Sweden', dial: '46', len: 9 },
  NO: { name: 'Norway', dial: '47', len: 8 },
  DK: { name: 'Denmark', dial: '45', len: 8 },
  FI: { name: 'Finland', dial: '358', len: 10 },
  PL: { name: 'Poland', dial: '48', len: 9 },
  CZ: { name: 'Czechia', dial: '420', len: 9 },
  RO: { name: 'Romania', dial: '40', len: 9 },
  HU: { name: 'Hungary', dial: '36', len: 9 },
  GR: { name: 'Greece', dial: '30', len: 10 },
  TR: { name: 'Turkey', dial: '90', len: 10 },
  RU: { name: 'Russia', dial: '7', len: 10 },
  UA: { name: 'Ukraine', dial: '380', len: 9 },
  IL: { name: 'Israel', dial: '972', len: 9 },
  EG: { name: 'Egypt', dial: '20', len: 10 },
  NG: { name: 'Nigeria', dial: '234', len: 10 },
  KE: { name: 'Kenya', dial: '254', len: 9 },
  ZA: { name: 'South Africa', dial: '27', len: 9 },
  MA: { name: 'Morocco', dial: '212', len: 9 },
  BR: { name: 'Brazil', dial: '55', len: 11 },
  MX: { name: 'Mexico', dial: '52', len: 10 },
  AR: { name: 'Argentina', dial: '54', len: 10 },
  CL: { name: 'Chile', dial: '56', len: 9 },
  CO: { name: 'Colombia', dial: '57', len: 10 },
  PE: { name: 'Peru', dial: '51', len: 9 },
  VE: { name: 'Venezuela', dial: '58', len: 10 },
  IR: { name: 'Iran', dial: '98', len: 10 },
  IQ: { name: 'Iraq', dial: '964', len: 10 },
  AF: { name: 'Afghanistan', dial: '93', len: 9 },
  MM: { name: 'Myanmar', dial: '95', len: 10 },
  KH: { name: 'Cambodia', dial: '855', len: 9 },
  LA: { name: 'Laos', dial: '856', len: 10 },
  MN: { name: 'Mongolia', dial: '976', len: 8 },
  KZ: { name: 'Kazakhstan', dial: '7', len: 10 },
  UZ: { name: 'Uzbekistan', dial: '998', len: 9 },
  AZ: { name: 'Azerbaijan', dial: '994', len: 9 },
  GE: { name: 'Georgia', dial: '995', len: 9 },
  AM: { name: 'Armenia', dial: '374', len: 8 },
  BY: { name: 'Belarus', dial: '375', len: 9 },
  MD: { name: 'Moldova', dial: '373', len: 8 },
  BG: { name: 'Bulgaria', dial: '359', len: 9 },
  RS: { name: 'Serbia', dial: '381', len: 9 },
  HR: { name: 'Croatia', dial: '385', len: 9 },
  SI: { name: 'Slovenia', dial: '386', len: 8 },
  SK: { name: 'Slovakia', dial: '421', len: 9 },
  LT: { name: 'Lithuania', dial: '370', len: 8 },
  LV: { name: 'Latvia', dial: '371', len: 8 },
  EE: { name: 'Estonia', dial: '372', len: 8 },
  IE: { name: 'Ireland', dial: '353', len: 9 },
  IS: { name: 'Iceland', dial: '354', len: 7 },
  LU: { name: 'Luxembourg', dial: '352', len: 9 },
  MT: { name: 'Malta', dial: '356', len: 8 },
  CY: { name: 'Cyprus', dial: '357', len: 8 },
  JO: { name: 'Jordan', dial: '962', len: 9 },
  LB: { name: 'Lebanon', dial: '961', len: 8 },
  SY: { name: 'Syria', dial: '963', len: 9 },
  YE: { name: 'Yemen', dial: '967', len: 9 },
  TZ: { name: 'Tanzania', dial: '255', len: 9 },
  UG: { name: 'Uganda', dial: '256', len: 9 },
  GH: { name: 'Ghana', dial: '233', len: 9 },
  ET: { name: 'Ethiopia', dial: '251', len: 9 },
};

export function getCountry(iso2) {
  const code = String(iso2 || 'IN').toUpperCase();
  return COUNTRIES[code] || null;
}

/**
 * Normalize any phone input to parts usable by Truecaller endpoints.
 * Accepts: "9334120373", "09334120373", "+919334120373", "919334120373", "0091..."
 */
export function normalizePhone(raw, iso2 = 'IN') {
  const country = getCountry(iso2);
  const digits = String(raw || '').replace(/\D/g, '');
  if (!digits) throw Object.assign(new Error('Invalid phone number: no digits found'), { status: 400 });

  let national = digits;
  // strip trunk prefix (leading 0 / 00)
  national = national.replace(/^0+/, '') || national;

  if (country) {
    const { dial, len } = country;
    // exact national length -> use as is
    if (national.length === len) {
      // ok
    } else if (national.startsWith(dial) && national.length === dial.length + len) {
      // has country dialing code prefix -> strip it
      national = national.slice(dial.length);
    }
    // else: pass through, Truecaller handles both formats
  }

  return {
    countryCode: country ? String(iso2).toUpperCase() : String(iso2).toUpperCase(),
    dialingCode: country ? Number(country.dial) : undefined,
    national: national || digits,
    e164: country ? `+${country.dial}${national}` : `+${digits}`,
  };
}
