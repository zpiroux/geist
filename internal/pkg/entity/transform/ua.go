package transform

import (
	"encoding/json"
	"github.com/mssola/user_agent"
	"net/url"
)

type UserAgent struct {
	Platform        string          `json:"platform"`
	OperatingSystem OperatingSystem `json:"operatingSystem"`
	Localization    string          `json:"localization"`
	Browser         Browser         `json:"browser"`
	Bot             bool            `json:"bot"`
	Mobile          bool            `json:"mobile"`
}

type OperatingSystem struct {
	Name     string `json:"name"`
	FullName string `json:"fullName"`
	Version  string `json:"version"`
}

type Browser struct {
	Name          string `json:"name"`
	Version       string `json:"version"`
	Engine        string `json:"engine"`
	EngineVersion string `json:"engineVersion"`
}

func NewUserAgent(uaString string) (UserAgent, error) {
	str, err := url.QueryUnescape(uaString)
	if err != nil {
		return UserAgent{}, err
	}
	ua := user_agent.New(str)
	os := ua.OSInfo()
	bName, bVersion := ua.Browser()
	eName, eVersion := ua.Engine()
	return UserAgent{
		Platform: ua.Platform(),
		OperatingSystem: OperatingSystem{
			Name:     os.Name,
			FullName: os.FullName,
			Version:  os.Version,
		},
		Localization: ua.Localization(),
		Browser: Browser{
			Name:          bName,
			Version:       bVersion,
			Engine:        eName,
			EngineVersion: eVersion,
		},
		Bot:    ua.Bot(),
		Mobile: ua.Mobile(),
	}, nil
}

func (u UserAgent) String() string {
	jsonStr, _ := json.Marshal(u)
	return string(jsonStr)
}
