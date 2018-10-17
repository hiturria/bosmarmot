package sqlsol

import "github.com/pkg/errors"

// SpecLoader loads spec files and parses them
func SpecLoader(specDir, specFile string) (*Parser, error) {

	var parser *Parser
	var err error

	if specDir == "" && specFile == "" {
		return nil, errors.New("One of SpecDir or SpecFile must be provided")
	}

	if specDir != "" && specFile != "" {
		return nil, errors.New("SpecDir or SpecFile must be provided, but not both")
	}

	if specDir != "" {
		parser, err = NewParserFromFolder(specDir)
		if err != nil {
			return nil, errors.Wrap(err, "Error parsing spec config folder")
		}
	} else {
		parser, err = NewParserFromFile(specFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error parsing spec config file")
		}
	}

	return parser, nil
}
