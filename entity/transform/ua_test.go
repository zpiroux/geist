package transform

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var uaStrings = []string{
	"Mozilla%2F5.0%20(Macintosh%3B%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F93.0.4577.63%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F93.0.4577.82%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Linux%3B%20Android%208.0.0%3B%20SM-G930F)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F94.0.4606.50%20Mobile%20Safari%2F537.36",
	"Mozilla%2F5.0%20(iPhone%3B%20CPU%20iPhone%20OS%2014_8%20like%20Mac%20OS%20X)%20AppleWebKit%2F605.1.15%20(KHTML%2C%20like%20Gecko)%20Mobile%2F15E148",
	"Mozilla%2F5.0%20(iPhone%3B%20CPU%20iPhone%20OS%2014_7_1%20like%20Mac%20OS%20X)%20AppleWebKit%2F605.1.15%20(KHTML%2C%20like%20Gecko)%20Mobile%2F15E148",
	"Mozilla%2F5.0%20(Windows%20NT%2010.0)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F88.0.4324.150%20Safari%2F537.36%20Edg%2F88.0.705.68",
	"Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F93.0.4577.82%20Safari%2F537.36%20Edg%2F93.0.961.52",
	"Mozilla%2F5.0%20(Linux%3B%20Android%2010%3B%20LM-K200%20Build%2FQKQ1.200311.002%3B%20wv)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Version%2F4.0%20Chrome%2F93.0.4577.82%20Mobile%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Linux%3B%20Android%2010%3B%20SNE-LX3)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F92.0.4515.115%20Mobile%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Linux%3B%20Android%2011%3B%20SM-A715F)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F93.0.4577.82%20Mobile%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F94.0.4606.61%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Linux%3B%20Android%2010%3B%20HRY-LX1%20Build%2FHONORHRY-L21%3B%20wv)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Version%2F4.0%20Chrome%2F93.0.4577.82%20Mobile%20Safari%2F537.36",
	"Mozilla%2F5.0%20(Macintosh%3B%20Intel%20Mac%20OS%20X%2010_15_6)%20AppleWebKit%2F605.1.15%20(KHTML%2C%20like%20Gecko)%20Version%2F14.1.2%20Safari%2F605.1.15",
	"Mozilla%2F5.0%20(iPhone%3B%20CPU%20iPhone%20OS%2014_6%20like%20Mac%20OS%20X)%20AppleWebKit%2F605.1.15%20(KHTML%2C%20like%20Gecko)%20Version%2F14.1.1%20Mobile%2F15E148%20Safari%2F604.1",
	"Mozilla%2F5.0%20(Windows%20NT%206.2%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F88.0.4324.182c%20(PSFooBrowserEmbedded)%20Safari%2F537.36",
}

func TestNewUserAgent(t *testing.T) {

	for _, ua := range uaStrings {
		_, err := NewUserAgent(ua)
		assert.NoError(t, err)
	}
	// TODO: add check for specific fields
}
